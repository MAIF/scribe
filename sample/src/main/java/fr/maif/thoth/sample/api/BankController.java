package fr.maif.thoth.sample.api;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import javax.sql.DataSource;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import fr.maif.eventsourcing.EventProcessor;
import fr.maif.eventsourcing.ProcessingSuccess;
import fr.maif.eventsourcing.TransactionManager;
import fr.maif.thoth.sample.commands.BankCommand;
import fr.maif.thoth.sample.events.BankEvent;
import fr.maif.thoth.sample.projections.eventualyconsistent.MeanWithdrawProjection;
import fr.maif.thoth.sample.state.Account;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.collection.Seq;
import io.vavr.concurrent.Future;
import io.vavr.control.Either;

@RestController
@RequestMapping("/bank/api")
public class BankController {
    private final EventProcessor<String, Account, BankCommand, BankEvent, Connection, Tuple0, Tuple0, Tuple0> eventProcessor;
    private final DataSource dataSource;
    private final MeanWithdrawProjection withdrawProjection;

    public BankController(
            EventProcessor<String, Account, BankCommand, BankEvent, Connection, Tuple0, Tuple0, Tuple0> eventProcessor,
            DataSource dataSource,
            MeanWithdrawProjection withdrawProjection) {
        this.eventProcessor = eventProcessor;
        this.dataSource = dataSource;
        this.withdrawProjection = withdrawProjection;
    }

    @GetMapping("/{id}")
    public CompletableFuture<ResponseEntity<AccountDTO>> readAccount(@PathVariable("id") String id) {
        return eventProcessor.getAggregate(id)
                .map(maybeAccount ->
                        maybeAccount.map(AccountDTO::fromAccount).map(ResponseEntity::ok)
                                .getOrElse(() -> new ResponseEntity<>(AccountDTO.error("Account does not exist"), HttpStatus.NOT_FOUND))
                ).toCompletableFuture();
    }

    @PostMapping("/")
    public CompletableFuture<ResponseEntity<AccountDTO>> openAccount(@RequestBody AccountDTO account) {
        return eventProcessor.processCommand(new BankCommand.OpenAccount(account.id, account.balance))
                .map(BankController::resultToDTO)
                .toCompletableFuture();
    }

    @PostMapping("/{id}/_action/withdraw")
    public CompletableFuture<ResponseEntity<AccountDTO>> withdraw(@PathVariable("id") String id, @RequestBody BalanceDTO withdraw) {
        return eventProcessor.processCommand(new BankCommand.Withdraw(id, withdraw.amount))
                .map(BankController::resultToDTO)
                .toCompletableFuture();
    }

    @PostMapping("/{id}/_action/deposit")
    public CompletableFuture<ResponseEntity<AccountDTO>> deposit(@PathVariable("id") String id, @RequestBody BalanceDTO deposit) {
        return eventProcessor.processCommand(new BankCommand.Deposit(id, deposit.amount))
                .map(BankController::resultToDTO)
                .toCompletableFuture();
    }


    @PostMapping("/_action/transfer")
    public ResponseEntity<TransferResultDTO> transfer(@RequestBody TransferDTO transferDTO) {
        if(Objects.isNull(transferDTO.from)) {
            return ResponseEntity.badRequest().body(TransferResultDTO.error("from field must be set"));
        } else if(Objects.isNull(transferDTO.to)) {
            return ResponseEntity.badRequest().body(TransferResultDTO.error("to field must be set"));
        } else if(Objects.isNull(transferDTO.amount)) {
            return ResponseEntity.badRequest().body(TransferResultDTO.error("amount field must be set"));
        }

        try(Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            var withdrawResult = eventProcessor
                    .batchProcessCommand(
                            connection,
                            List.of(new BankCommand.Withdraw(transferDTO.from, transferDTO.amount))
                    );
            var depositResult = eventProcessor
                    .batchProcessCommand(
                            connection,
                            List.of(new BankCommand.Deposit(transferDTO.to, transferDTO.amount))
                    );

            return withdrawResult.zip(depositResult).map(tuple ->
                tuple._1.and(tuple._2, (leither1, leither2) -> {
                    var mergedResult = Either
                            .sequence(List.of(leither1.head(), leither2.head()));
                    try {
                        if(mergedResult.isLeft()) {
                            connection.rollback();
                        } else {
                            connection.commit();
                        }
                    } catch (SQLException exception) {
                        return new ResponseEntity<>(TransferResultDTO.error("Failed to commit / rollback"), HttpStatus.INTERNAL_SERVER_ERROR);
                    }
                    return transferResultToDTO(mergedResult, transferDTO.from, transferDTO.to);
                }

            )).flatMap(TransactionManager.InTransactionResult::postTransaction)
            .toCompletableFuture().join();

        } catch (SQLException throwables) {
            return new ResponseEntity<>(TransferResultDTO.error("Failed to open connection"), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @DeleteMapping("/{id}")
    public CompletableFuture<ResponseEntity<?>> closeAccount(@PathVariable("id") String id) {
        return eventProcessor.processCommand(new BankCommand.CloseAccount(id))
                .map(either -> either.fold(
                        error -> ResponseEntity.badRequest().body(AccountDTO.error(error)),
                        __ -> ResponseEntity.noContent().build()))
                .toCompletableFuture();
    }

    @GetMapping("/balance")
    public BalanceDTO globalBalance() throws SQLException {
        try(final ResultSet resultSet = dataSource.getConnection().prepareStatement("SELECT balance::numeric FROM global_balance LIMIT 1").executeQuery()) {
            resultSet.next();
            BigDecimal amount = resultSet.getBigDecimal(1);
            BalanceDTO response = new BalanceDTO();
            response.amount = amount;

            return response;
        }
    }

    @GetMapping("/stats/{id}")
    public BalanceDTO meanWithdrawByUser(
            @PathVariable("id") String id,
            @RequestParam(name = "month", required = false) String month,
            @RequestParam(name = "year", required = false) Integer year
    ) throws SQLException {
        Optional<BigDecimal> maybeAmount;
        if(Objects.isNull(month) || Objects.isNull(year)) {
            maybeAmount = withdrawProjection.meanWithdrawByClient(id);
        } else {
            maybeAmount = withdrawProjection.meanWithdrawByClientAndMonth(id, year, month);
        }

        return maybeAmount.map(amount -> {
            BalanceDTO result = new BalanceDTO();
            result.amount = amount;
            return result;
        }).orElseGet(() -> {
            BalanceDTO result = new BalanceDTO();
            result.error = "No withdraw data available";
            return result;
        });
    }

    private static ResponseEntity<TransferResultDTO> transferResultToDTO(Either<
            Seq<String>,
            Seq<ProcessingSuccess<Account, BankEvent, Tuple0, Tuple0, Tuple0>>> maybeSuccess,
            String from, String to) {
        return maybeSuccess.fold(
                errors -> ResponseEntity.badRequest().body(TransferResultDTO.error(String.join(",", errors))),
                success -> ResponseEntity.ok(success.foldLeft(new TransferResultDTO(), (result, processingSuccess) -> {
                        final Account account = processingSuccess.currentState.get();
                        final AccountDTO dto = AccountDTO.fromAccount(account);
                        if(dto.id.equals(from)) {
                            result.from = AccountDTO.fromAccount(account);
                        } else if(dto.id.equals(to)) {
                            result.to = AccountDTO.fromAccount(account);
                        }

                        return result;
                    }))
        );
    }

    private static ResponseEntity<AccountDTO> resultToDTO(Either<
            String,
            ProcessingSuccess<
                    Account,
                    BankEvent,
                    Tuple0, Tuple0, Tuple0
                    >
            > maybeSuccess) {
        return maybeSuccess.fold(
                error -> ResponseEntity.badRequest().body(AccountDTO.error(error)),
                success -> success.currentState.map(AccountDTO::fromAccount).map(ResponseEntity::ok).get()
        );
    }
}

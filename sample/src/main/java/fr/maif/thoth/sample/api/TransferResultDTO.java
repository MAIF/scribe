package fr.maif.thoth.sample.api;

public class TransferResultDTO {
    public AccountDTO from;
    public AccountDTO to;
    public String error;

    public TransferResultDTO() { }

    public TransferResultDTO(AccountDTO from, AccountDTO to) {
        this.from = from;
        this.to = to;
    }

    public static TransferResultDTO error(String error) {
        final TransferResultDTO result = new TransferResultDTO();
        result.error = error;

        return result;
    }
}

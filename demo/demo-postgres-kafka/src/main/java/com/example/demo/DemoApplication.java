package com.example.demo;

import akka.actor.ActorSystem;

import java.math.BigDecimal;
import java.sql.SQLException;

public class DemoApplication {

	public static void main(String[] args) throws SQLException {
		ActorSystem actorSystem = ActorSystem.create();
		BankCommandHandler commandHandler = new BankCommandHandler();
		BankEventHandler eventHandler = new BankEventHandler();
		Bank bank = new Bank(actorSystem, commandHandler, eventHandler);

		String id = bank.createAccount(BigDecimal.valueOf(100)).get().get().currentState.get().id;

		bank.withdraw(id, BigDecimal.valueOf(50)).get().get().currentState.get();
		BigDecimal balance = bank.withdraw(id, BigDecimal.valueOf(10)).get().get().currentState.get().balance;
		System.out.println(balance);

		System.out.println(bank.meanWithdrawValue());
	}

}

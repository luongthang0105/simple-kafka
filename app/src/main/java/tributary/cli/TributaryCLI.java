package tributary.cli;

import java.util.Scanner;

public class TributaryCLI {
    public static void main(String[] args) {
        System.out.println("Welcome to the Tributary CLI!");
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.print("> ");
            String command = scanner.nextLine();
            if (command.startsWith("exit")) {
                System.out.println("Exiting the TributaryCLI...");
                break;
            } else if (command.startsWith("create")) {
                Create.processCreation(command);
            } else if (command.startsWith("delete")) {
                Delete.processDelete(command);
            } else if (command.startsWith("produce")) {
                Produce.processProduce(command);
            } else if (command.startsWith("consume")) {
                Consume.processConsume(command);
            } else if (command.startsWith("show")) {
                Show.processShow(command);
            } else if (command.startsWith("set")) {
                Set.processSet(command);
            }
        }
        scanner.close();

    }
}

package tributary.cli;

import java.util.Scanner;

import tributary.core.TributaryController;

public class TributaryCLI {
    public static void main(String[] args) {
        System.out.println("Welcome to the Tributary CLI!");
        Scanner scanner = new Scanner(System.in);
        TributaryController controller = new TributaryController();
        while (true) {
            System.out.print("> ");
            String command = scanner.nextLine();
            if (command.startsWith("exit")) {
                System.out.println("Exiting the TributaryCLI...");
                break;
            } else if (command.startsWith("create")) {
                Create.processCreation(command, controller);
            } else if (command.startsWith("delete")) {
                Delete.processDelete(command, controller);
            } else if (command.startsWith("produce")) {
                Produce.processProduce(command, controller);
            } else if (command.startsWith("consume")) {
                Consume.processConsume(command, controller);
            } else if (command.startsWith("show")) {
                Show.processShow(command, controller);
            } else if (command.startsWith("set")) {
                Set.processSet(command, controller);
            } else if (command.startsWith("parallel")) {
                Parallel.processParallel(command, controller);
            }
        }
        scanner.close();

    }
}

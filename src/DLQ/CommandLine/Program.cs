using DLQ;
using System;
using System.CommandLine;
using System.CommandLine.Invocation;

public class Program
{
    public static async Task Main(string[] args)
    {
        // https://docs.microsoft.com/en-us/archive/msdn-magazine/2019/march/net-parse-the-command-line-with-system-commandline
        // https://github.com/dotnet/command-line-api
        // https://github.com/dotnet/command-line-api/blob/main/docs/Your-first-app-with-System-CommandLine.md

        var queueNameOption = new Option<string>("--QueueName", "The name of the Service Bus Queue.");
        queueNameOption.AddAlias("-q");

        var connectionStringOption = new Option<string>("--ConnectionString", "The connection string to the Service Bus Queue.");
        connectionStringOption.AddAlias("-c");


        RootCommand rootCommand = new RootCommand(description: "Clears the DLQ")
        {
            queueNameOption,
            connectionStringOption
        };

        var loadTestMsgCommand = new Command("test", "Utility to create and send test messages and move them to the DLQ.");


        var clearDLQCommand = new Command("clear", "Utility to clear the dead-letter queue.");

        rootCommand.Add(loadTestMsgCommand);
        rootCommand.Add(clearDLQCommand);

        loadTestMsgCommand.SetHandler(async (string queueName, string connectionString) =>
        {
            // command line args, example:
            // --QueueName Test
            // --ConnectionString "Endpoint=sb://my connection string"
            // test

            Console.WriteLine("test");
            Console.WriteLine($"QueueName = {queueName}");
            Console.WriteLine($"ConnectionString = {connectionString}");
            Console.WriteLine("");

            QueueSender sender = new QueueSender(connectionString);

            var msgs = new List<string>();
            for (int i = 0; i < 55; i++)
            {
                msgs.Add($"Test Message {i}");
            }

            // create test messages
            Console.Write($"Sending {msgs.Count()} messages for testing purposes...");
            await sender.SendBatch(queueName, msgs);
            Console.WriteLine("DONE.");

            // deadletter them to test clearing
            long activeMsgCount = await sender.GetActiveMessageCount(queueName);
            Console.Write($"Deadlettering all {activeMsgCount} active messages in queue: {queueName}...");
            await sender.DeadLetterAllTestMessages(queueName);
            Console.WriteLine("DONE.");

        }, queueNameOption, connectionStringOption);


        clearDLQCommand.SetHandler(async (string queueName, string connectionString) =>
        {
            // command line args, example:
            // --QueueName Test
            // --ConnectionString "Endpoint=sb://my connection string"
            // clear

            Console.WriteLine("clear");
            Console.WriteLine($"QueueName = {queueName}");
            Console.WriteLine($"ConnectionString = {connectionString}");
            Console.WriteLine();

            QueueClearer clearer = new QueueClearer(connectionString);

            await clearer.Clear(queueName);

        }, queueNameOption, connectionStringOption);

        rootCommand.SetHandler( async (string queueName, string connectionString) =>
        {
            Console.WriteLine("");
            Console.WriteLine($"QueueName = {queueName}");
            Console.WriteLine($"ConnectionString = {connectionString}");
            Console.WriteLine();
        }, queueNameOption, connectionStringOption);


        await rootCommand.InvokeAsync(args);
    }

}
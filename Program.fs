open Argu
open Microsoft.Azure.ServiceBus
open System
open System.Threading.Tasks
open FSharp.Control.Tasks
open System.Threading

type Arguments =
| [<Mandatory>] ConnectionString of string
| [<Mandatory>] Queue of string
| MessageCount of int
with
  interface IArgParserTemplate with
    member __.Usage =
      match __ with
      | ConnectionString _ -> "service bus connection string"
      | Queue _ -> "path to a queue"
      | MessageCount _ -> "number of messages to handle concurrently"

let handleError (args : ExceptionReceivedEventArgs) =
  raise args.Exception
  Task.CompletedTask

let handleMessage (send : Message -> Task) (complete : string -> Task) (msg : Message) _ =
  task {
    let messageId = sprintf "%s:%i" msg.MessageId <| DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
    let newMsg = Message(msg.Body, MessageId = messageId)
    printfn "Enqueueing %s as %s" msg.MessageId newMsg.MessageId
    do! send newMsg
    return! complete msg.SystemProperties.LockToken
  } :> Task

let processMessages connectionString queue (messageCount : int) =
  let deadLetterClient = QueueClient(connectionString, EntityNameHelper.FormatDeadLetterPath queue)
  let queueClient = QueueClient(connectionString, queue)
  let handleError = Func<ExceptionReceivedEventArgs, Task> handleError
  let processMessage = Func<Message, CancellationToken, Task> (handleMessage queueClient.SendAsync deadLetterClient.CompleteAsync)
  let options = MessageHandlerOptions(handleError, MaxConcurrentCalls = messageCount)
  deadLetterClient.RegisterMessageHandler (processMessage, options)

[<EntryPoint>]
let main argv =
  let parser = ArgumentParser.Create<Arguments>()
  try
    let results = parser.Parse argv
    let connectionString = results.GetResult <@ ConnectionString  @>
    let queue = results.GetResult <@ Queue @>
    let messageCount = results.GetResult(<@ MessageCount @>, 1)
    processMessages connectionString queue messageCount
  with
  | :? ArguParseException as ex -> printfn "%s" ex.Message
  | ex -> raise ex
  Console.ReadKey() |> ignore
  0
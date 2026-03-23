// package mccct

// import mccct.Async
// import gears.async
// import gears.async.default.given

// object MainAsyncFinish {
//   @main
//   def runAsync(): Unit =
//     println("Main running with AsyncFinish...")
//     Scheduler.start(RandomWalk, false)

//     async.Async.blocking:
//       Finish {
//         println("First finish 1.1...")
//         Async {
//           println("task 1 running...")
//           Finish {
//             println("task 3 running...")
//           }
//           Finish {
//             Async {
//               println("Task 4 running")
//             }
//           }
//           println("After inner finish")
//         }
//       }
//       println("After outer finish")
//       Async {
//         println("task 2 running...")
//       }

//     Scheduler.awaitTermination()
//     println(s"${Scheduler.getSchedule()}")
// }

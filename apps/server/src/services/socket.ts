import { Server } from "socket.io";
import Redis from "ioredis";
import prismaClient from "./prisma";

const pub = new Redis({
  host: "redis-253a7749-redis-bansal.a.aivencloud.com",
  username: "default",
  password: "AVNS_9LTwMARLgIleVld0tYF",
  port: 23745,
});
const sub = new Redis({
  host: "redis-253a7749-redis-bansal.a.aivencloud.com",
  username: "default",
  password: "AVNS_9LTwMARLgIleVld0tYF",
  port: 23745,
});

class SocketService {
  private _io: Server;

  constructor() {
    console.log("Init Socket ");
    this._io = new Server({
      cors: {
        allowedHeaders: ["*"],
        origin: "*",
      },
    });
    sub.subscribe("MESSAGES");
  }

  public initListeners() {
    const io = this.io;
    console.log("Init Socket Listeners....");
    io.on("connect", (socket) => {
      console.log(`New Socket connected`, socket.id);

      socket.on("event:message", async ({ message }: { message: string }) => {
        console.log("New MEssage Recieved", message);
        //publish message to redis
        await pub.publish("MESSAGES", JSON.stringify({ message }));
      });
    });

    sub.on("message", async (channel, message) => {
      if (channel === "MESSAGES") {
        io.emit("message", message);
        await prismaClient.message.create({
          data: {
            text: message,
          },
        });
      }
    });
  }

  get io() {
    return this._io;
  }
}

export default SocketService;

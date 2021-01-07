package com.bjmashibing.demo.io.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.Getter;
import lombok.Setter;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class MyRPCTest {
    @Test
    public void startServer() {
        NioEventLoopGroup boss = new NioEventLoopGroup(1);
        NioEventLoopGroup worker = boss;
        ServerBootstrap sbs = new ServerBootstrap();
        ChannelFuture bind = sbs.group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        System.out.println("server accept client port:" + ch.remoteAddress().getPort());
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new ServerDecode());
                        p.addLast(new ServerRequestHandler());
                    }
                }).bind(new InetSocketAddress("localhost", 9090));
        try {
            bind.sync().channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //模拟consumer端
    @Test
    public void get() {
        AtomicInteger num = new AtomicInteger(0);
        int size = 20;
        Thread[] threads = new Thread[size];
        for (int i = 0; i < size; i++) {
            threads[i] = new Thread(() -> {
                Car car = proxyGet(Car.class);
                car.ooxx("hello" + num.incrementAndGet());
            });
        }
        /*new Thread(() -> {
            startServer();
        }).start();
        //动态代理实现
        Car car = proxyGet(Car.class);
        car.ooxx("hello");
        //动态代理实现
        Fly fly = proxyGet(Fly.class);
        fly.ooxx("hello");*/
    }

    public static <T> T proxyGet(Class<T> interfaceInfo) {
        //实现各个版本的动态代理。。。。
        ClassLoader loader = interfaceInfo.getClassLoader();
        Class<?>[] methodInfo = {interfaceInfo};
        return (T) Proxy.newProxyInstance(loader, methodInfo, new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                //如何设计我们的consumer对于provider的调用过程
                //1.调用服务,方法,参数 ===>封装成message [content]
                String name = interfaceInfo.getName();
                String methodName = method.getName();
                Class<?>[] parameterTypes = method.getParameterTypes();
                MyContent content = new MyContent();
                content.setArgs(args);
                content.setName(name);
                content.setMethodName(methodName);
                content.setParameterTypes(parameterTypes);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ObjectOutputStream oout = new ObjectOutputStream(out);
                oout.writeObject(content);
                byte[] msgBody = out.toByteArray();
                //2.requestID+message,本地要缓存
                //协议:【header<>】 【msgBody】
                MyHeader header = createHeader(msgBody);
                out.reset();
                oout = new ObjectOutputStream(out);
                oout.writeObject(header);
                byte[] msgHeader = out.toByteArray();
                System.out.println("msgHeader:" + msgHeader.length);
                //3.连接池::取得连接
                ClientFactory factory = ClientFactory.getFactory();
                NioSocketChannel clientChannel = factory.getClient(new InetSocketAddress("localhost", 9090));
                //获取连接过程中:开始-创建  ， 过程-直接取

                //4.发送--> 走IO   out ---》走netty(event驱动)
                ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(msgHeader.length + msgBody.length);

                CountDownLatch countDownLatch = new CountDownLatch(1);
                long id = header.getRequestID();
                ResponseHandler.addCallBack(id, new Runnable() {
                    @Override
                    public void run() {
                        countDownLatch.countDown();
                    }
                });
                byteBuf.writeBytes(msgHeader);
                byteBuf.writeBytes(msgBody);
                clientChannel.writeAndFlush(msgHeader);
                ChannelFuture channelFuture = clientChannel.writeAndFlush(byteBuf);
                channelFuture.sync();  //io是双向的，你看似有个sync,她仅代表out

                countDownLatch.await();

                //5.?如果从IO,未来回来了，怎么将代码执行到这里
                //(睡眠/回调，如何让线程停下来?你还能让他继续...)
                return null;
            }
        });
    }

    public static MyHeader createHeader(byte[] msg) {
        MyHeader header = new MyHeader();
        int size = msg.length;
        int f = 0x14141414;
        long requestID = Math.abs(UUID.randomUUID().getLeastSignificantBits());
        //0x14 0001 0100
        header.setFlag(f);
        header.setDataLen(size);
        header.setRequestID(requestID);
        return header;
    }

}


//源于   spark  源码
class ClientFactory {
    int poolSize = 1;
    NioEventLoopGroup clientWorker;
    Random rand = new Random();

    private ClientFactory() {
    }

    private static final ClientFactory factory;

    static {
        factory = new ClientFactory();
    }

    public static ClientFactory getFactory() {
        return factory;
    }

    //一个consumer 可以连接很多的provider,间接的provider都有自己的pool    K,V
    ConcurrentHashMap<InetSocketAddress, ClientPool> outboxs = new ConcurrentHashMap<>();

    public synchronized NioSocketChannel getClient(InetSocketAddress address) {
        ClientPool clientPool = outboxs.get(address);
        if (clientPool == null) {
            outboxs.putIfAbsent(address, new ClientPool(poolSize));
            clientPool = outboxs.get(address);
        }
        int i = rand.nextInt(poolSize);

        if (clientPool.clients[i] != null && clientPool.clients[i].isActive()) {
            return clientPool.clients[i];
        }

        synchronized (clientPool.lock[i]) {
            return clientPool.clients[i] = create(address);
        }
    }

    private NioSocketChannel create(InetSocketAddress address) {
        //基于  netty   的客户端 创建方式
        clientWorker = new NioEventLoopGroup(1);
        Bootstrap bs = new Bootstrap();
        ChannelFuture connect = bs.group(clientWorker).channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new ClientResponses());//解决给谁的???  requestID

                    }
                }).connect(address);
        try {
            NioSocketChannel client = (NioSocketChannel) connect.sync().channel();
            return client;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

}

class ServerDecode extends ByteToMessageDecoder {

    //父类里一定有channelread{  前老的拼buf decode();剩余留存;对out遍历  } --> bytebuf
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) throws Exception {
        System.out.println("channel start:" + buf.readableBytes());
        while (buf.readableBytes() >= 110) {
            byte[] bytes = new byte[110];
            buf.getBytes(buf.readerIndex(), bytes);
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            ObjectInputStream oin = new ObjectInputStream(in);
            MyHeader header = (MyHeader) oin.readObject();
            System.out.println("server response @id:" + header.getRequestID());
            if (buf.readableBytes() >= header.getDataLen()) {
                buf.readBytes(110);
                byte[] data = new byte[(int) header.getDataLen()];
                buf.readBytes(data);
                ByteArrayInputStream din = new ByteArrayInputStream(data);
                ObjectInputStream doin = new ObjectInputStream(din);
                MyContent content = (MyContent) doin.readObject();
                System.out.println(content.getName());
                out.add(new Packmsg(header, content));
            } else {
                break;
            }
        }
    }
}

class ClientPool {
    NioSocketChannel[] clients;
    Object[] lock;

    ClientPool(int size) {
        clients = new NioSocketChannel[size];//init 连接都是空的
        lock = new Object[size];//锁是可以初始化的
        for (int i = 0; i < size; i++) {
            lock[i] = new Object();
        }
    }
}

class ResponseHandler {
    static ConcurrentHashMap<Long, Runnable> mapping = new ConcurrentHashMap<>();

    public static void addCallBack(long requestID, Runnable cb) {
        mapping.putIfAbsent(requestID, cb);
    }

    public static void runCallBack(long requestID) {
        Runnable runnable = mapping.get(requestID);
        runnable.run();
        removeCB(requestID);
    }

    private static void removeCB(long requestID) {
        mapping.remove(requestID);
    }
}

class ClientResponses extends ChannelInboundHandlerAdapter {
    //consumer
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf buf = (ByteBuf) msg;
        if (buf.readableBytes() >= 110) {
            byte[] bytes = new byte[110];
            buf.readBytes(bytes);
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            ObjectInputStream oin = new ObjectInputStream(in);
            MyHeader header = (MyHeader) oin.readObject();
            System.out.println("client response @ id" + header.getRequestID());
            //TODO:
            ResponseHandler.runCallBack(header.requestID);
            /*if (buf.readableBytes() >= header.getDataLen()) {
                byte[] data = new byte[(int) header.getDataLen()];
                buf.readBytes(data);
                ByteArrayInputStream din = new ByteArrayInputStream(data);
                ObjectInputStream doin = new ObjectInputStream(din);
                MyContent content = (MyContent) doin.readObject();
                System.out.println(content.getName());
            }*/
        }
        super.channelRead(ctx, msg);
    }
}

class ServerRequestHandler extends ChannelInboundHandlerAdapter {
    //provider
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Packmsg requestPkg = (Packmsg) msg;
        System.out.println("server handler:" + requestPkg.content.getArgs()[0]);
        //如果假设处理完了，要给客户端返回了!!!

        //因为是个RPC，你得有requestID
        //在client那一侧也要解决解码问题
        //关注rpc通信协议   来的时候flag 0x14141414
        //io是双向的
        //有新的header+content
        String ioThreadName = Thread.currentThread().getName();
        //1.直接在当前方法   处理IO和业务和返回
        //2.使用netty自己的eventloop来处理业务及返回
        ctx.executor().execute(new Runnable(){
            @Override
            public void run(){
                String execThreadName = Thread.currentThread().getName();
                MyContent content = new MyContent();
                content.setRes("");
                MyHeader resHeader = new MyHeader();
                resHeader.setRequestID(requestPkg.header.getRequestID()):
                resHeader.setFlag(0x14141414);
                resHeader.setDataLen();
                ctx.writeAndFlush(ooxx);
            }
        });

        /*ByteBuf buf = (ByteBuf) msg;
        ByteBuf sendBuf = buf.copy();
        if (buf.readableBytes() >= 110) {
            byte[] bytes = new byte[110];
            buf.readBytes(bytes);
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            ObjectInputStream oin = new ObjectInputStream(in);
            MyHeader header = (MyHeader) oin.readObject();
            System.out.println("server response @id:" + header.getRequestID());
            //TODO:
            ResponseHandler.runCallBack(header.requestID);
            if (buf.readableBytes() >= header.getDataLen()) {
                byte[] data = new byte[(int) header.getDataLen()];
                buf.readBytes(data);
                ByteArrayInputStream din = new ByteArrayInputStream(data);
                ObjectInputStream doin = new ObjectInputStream(din);
                MyContent content = (MyContent) doin.readObject();
                System.out.println(content.getName());
            }
        }
        ChannelFuture channelFuture = ctx.writeAndFlush(sendBuf);
        channelFuture.sync();*/
    }
}

@Getter
@Setter
class MyHeader implements Serializable {
    //通信上的协议
  /*
  1.ooxx值
  2.UUID:requestID
  3.DATA_LEN
  */
    int flag; //32bit可以设置很多的信息。。
    long requestID;
    long dataLen;
}

@Getter
@Setter
class MyContent implements Serializable {
    String name;
    String methodName;
    Class<?>[] parameterTypes;
    Object[] args;
    String res;
}

interface Car {
    void ooxx(String msg);
}

interface Fly {
    void ooxx(String msg);
}


package com.bjmashibing.demo.io.netty;

public class MyRPCTest {
    //模拟consumer端
    @Test
    public void get(){
        //动态代理实现
        Car car= proxyGet(Car.class);
        car.ooxx("hello");
        //动态代理实现
        Fly fly= proxyGet(Fly.class);
        cat.ooxx("hello");
    }
    public static <T>T proxyGet(Class<T> interfaceInfo){
        //实现各个版本的动态代理。。。。
        ClassLoader loader = interfaceInfo.getClassLoader();
        Class<?>[] methodInfo = {interfaceInfo};
        return Proxy.newProxyInstance(loader,methodInfo,new InvocationHandler(){
            @Override
            public Object invoke(Object proxy,Method method,Object[] args)throws    	Throwable{
                //如何设计我们的consumer对于provider的调用过程
                //1.调用服务,方法,参数 ===>封装成message [content]
                String name = interfaceInfo.getName();
                String methodName = method.getName();
                Class<?>[] parameterTypes=method.getParameterTypes();
                MyContent content = new MyContent();
                content.setArgs(args);
                content.setName(name);
                content.setMethodName(methodName);
                content.setParameterTypes(paramterTypes);
                ByteArrayOutputStream out=new ByteArrayOutputStream();
                ObjectOutputStream oout=new ObjectOutputStream(out);
                oout.writeObject(content);
                byte[] msgBody=out.toByteArray();
                //2.requestID+message,本地要缓存
                //协议:【header<>】 【msgBody】
                MyHeader header = createHeader(msgBody);
                out.reset();
                oout = new ObjectOutputStream(out);
                oout.writeObject(header);
                byte[] msgHeader = out.toByteArray();

                //3.连接池::取得连接
                ClientFactory factory=ClientFactory.getFactory();
                factory.getClient();
                //4.发送--> 走IO   out ---》走netty(event驱动)
                channel.writeAndFlush(ByteBuf);
                //5.?如果从IO,未来回来了，怎么将代码执行到这里
                //(睡眠/回调，如何让线程停下来?你还能让他继续...)
                return null;
            }
        });
    }
    public static MyHeader createHeader(byte[] msg){
        MyHeader header = new MyHeader();
        int size = msg.length;
        int f= 0x14141414;
        long requestID = Math.abs(UUID.randomUUID().getLeastSignificantBits());
        //0x14 0001 0100
        header.setFlag(f);
        header.setDataLen(size);
        header.setRequestID(requestID);
        return header;
    }
    //源于   spark  源码
    class ClientFactory{
        int poolSize = 1;
        NioEventLoopGroup clientWorker;
        Random rand = new Random();
        ClientFactory factory;
        static {
            factory = new ClientFactory();
        }
        public static ClientFactory getFactory(){
            return factory;
        }
        //一个consumer 可以连接很多的provider,间接的provider都有自己的pool    K,V
        ConcurrentHashMap<InetSocketAddress,ClientPool> outBoxs=new ConcurrentHashMap<>();
        public synchronized NioSocketChannel getFactory(InetSocketAddress address){
            ClientPool clientPool = outboxs.get(address);
            if(clientPool==null){
                outboxs.putIfAbsent(address,new ClientPool(poolSize));
                clientPool=outboxs.get(address);
            }
            int i = rand.nextInt(poolSize);

            if(clientPool.clients[i]!=null && clientPool.clients[i].isActive()){
                return clientPool.clients[i];
            }

            synchronized (clientPool.lock[i]){
                return clientPool.clients[i]=create(address);
            }

            private NioSocketChannel create(InetSocketAddress address){
                //基于  netty   的客户端 创建方式
                clientWorker=new NioEventLoopGroup(1);
                Bootstrap bs= new Bootstrap();
                bs.group(clientWorker).channel(NioSocketChannel.class)
                        .handler(new ChannelInitializer<NioSocketChannel>(){
                            @Override
                            protected void initChannel(NioSocketChannel ch)throws Exception{
                                ChannelPipeline p =ch.pipeline();
                                p.addLast(new ClientResponses());//解决给谁的???

                            }
                        });
            }
        }

    }
    class ClientResponses extends ChannelInboundHandlerAdapter{
        @Override
        public void channelRead(ChannelHandlerContext ctx,Object msg)throws Exception{
            ByteBuf buf = (ByteBuf)msg;

            super.channelRead(ctx,msg);
        }
    }
    class ClientPool{
        NioSocketChannel[] clients;
        Object[] lock;
        ClientPool(int size){
            clients = new NioSocketChannel[size];//init 连接都是空的
            lock=new Object[size];//锁是可以初始化的
            for(int i =0;i<size;i++){
                lock[i]=new Object();
            }
        }
    }
    class MyHeader implements Serializable{
        //通信上的协议
  /*
  1.ooxx值
  2.UUID:requestID
  3.DATA_LEN
  */
        int flag; //32bit可以设置很多的信息。。
        long requestId;
        long dataLen;
        set/get
    }
}
class MyContent implements Serializable{
    String name;
    String methodName;
    Class<?>[] paramterTypes;
    Object[] args;
    get/set
}
interface Car{
    void xxoo(String msg);
}
interface Fly{
    void xxoo(String msg);
}
}

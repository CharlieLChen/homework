# homework
用Pravega实现1对1聊天功能，同时实现了文件的上传下载功能。对于群组聊天其实就是针对一个stream的读写，相对于1v1简单一些，本程序没有实现，读者有兴趣的，可以拿1v1的程序改一改就有了。


# 实现细节

1. 对于1v1聊天，分别为每个人创建一个stream，每个人只能往自己的stream写，只能读对方的stream。

2. 定义Message类，消息类型为Normal和Upload两种类型的Message，通过JavaSerializer把Message序列化后写入自己的stream.


# 使用指导

1. 通过命令行传入自己的名字和对方的名字，启动client之后，即可进行输入对话，但是要等到对方上线以后才能你发的看到消息。
2. 通过`upload@C:\Users\chenc49\OneDrive - Dell Technologies\Desktop\param\zk.txt` 格式的输入上传文件，文件将被下载到`C:\Users\chenc49\OneDrive - Dell Technologies\Desktop\chat\$self`目录下面

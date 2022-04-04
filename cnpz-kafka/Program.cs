using cnpz_kafka.GenericKafka;

void myCallback(string str) {
	Console.WriteLine(str);
}

Consumer.ReadKafka("downloader", myCallback);
//var envio = Producer.SendKafka("downloader", "mensagem 1");
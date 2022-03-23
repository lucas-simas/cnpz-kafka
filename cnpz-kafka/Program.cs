using cnpz_kafka.GenericKafka;

void myCallback(string str) {
	Console.WriteLine(str);
}

Consumer.ReadKafka("topico1", myCallback);
//var envio = Producer.SendKafka("topico1", "mensagem 1");
//await kafka.SendKafka();
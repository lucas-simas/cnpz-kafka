using Confluent.Kafka;

namespace cnpz_kafka.GenericKafka
{
    /// <summary>
    /// Classe responsável para chamadas aos producers (enviar mensagem) do KAFKA
    /// </summary>
    public class Producer
    {
        /// <summary>
        /// Task para envio de mensagem ao KAFKA
        /// </summary>
        /// <param name="topic">Tópico da mensagem kafka</param>
        /// <param name="message">Mensagem que será enviada</param>
        /// <param name="ip_port">IP e Porta para endereço da mensagem + topico</param>
        static public Boolean SendKafka( string topic, string message,  string ip_port = "127.0.0.1:9094" ) {

            var config = new ProducerConfig { BootstrapServers = ip_port };
            using var p = new ProducerBuilder<Null, string>(config).Build();

            try {
                p.Produce(topic, new Message<Null, string> { Value = message });
            }
            catch (ProduceException<Null, string> e) {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                return false;
            }

            return true;

        }
    }
}

# Security & Certificates

The `KafkaCredentialStore` can be used to create the necessary trust stores and passwords to access Kafka.

The parameters passed in are string representations of the client private key, client certificate
and service certificate. the `properties` field in `KafkaCredentialStore` can then be applied to
any of the `*Settings` classes by using the `withProperties(kafkaCredentialStore.properties)`.

```scala mdoc
import cats.effect._
import fs2.kafka._
import fs2.kafka.security._

def createKafkaProducerUsingPem[F[_]: Sync, K, V](
  caCertificate: String,
  accessKey: String,
  accessCertificate: String
)(implicit keySer: Serializer[F, K], valSer: Serializer[F, V]): ProducerSettings[F, K, V] =
  ProducerSettings[F, K, V].withCredentials(
    KafkaCredentialStore.fromPemStrings(
      caCertificate,
      accessKey,
      accessCertificate
    )
  )
```

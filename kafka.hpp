#pragma once

#include <librdkafka/rdkafkacpp.h>

#include <cstring>
#include <inttypes.h>
#include <map>
#include <memory>
#include <stdexcept>
#include <sstream>

namespace Kafka {

namespace {

class StdLogger {
public:
    template <typename TMsg>
    void info(TMsg msg) {
        std::cout << msg << "\n";
    }
    template <typename TMsg>
    void error(TMsg msg) {
        std::cerr << msg << "\n";
    }
    template <typename TMsg>
    void debug(TMsg msg) {
        std::cout << msg << "\n";
    }
};


}   //  noname namespace

class KafkaSsl {
    std::string broker_certificate_location_;
    std::string client_certificate_location_;
    std::string key_location_;
    std::string client_name_;
    std::string client_password_;

public:
    KafkaSsl(::RdKafka::Conf& g_conf,
             const char* broker_certificate_location,
             const char* client_certificate_location,
             const char* client_key,
             const char* client_name = "",
             const char* client_password = "")
            : broker_certificate_location_(broker_certificate_location)
            , client_certificate_location_(client_certificate_location)
            , key_location_(client_key)
            , client_name_(client_name)
            , client_password_(client_password)
    {
        /*
            security.protocol=ssl

            # CA certificate file for verifying the broker's certificate.
            ssl.ca.location=ca-cert

            # Client's certificate
            ssl.certificate.location=client_?????_client.pem

            # Client's key
            ssl.key.location=client_?????_client.key

            # Key password, if any.
            ssl.key.password=abcdefgh
         */
        auto set_config = [&g_conf](const char* param, const char* value) {
            std::string errstr;
            if (g_conf.set(param, value, errstr) !=
                RdKafka::Conf::CONF_OK) {
                throw std::runtime_error(std::string("Kafka producer set ") + param + " value " + value + " failed: " + errstr);
            }
        };
        //set_config("debug", "fetch,security,protocol,broker");
        set_config("security.protocol", "SASL_SSL");
        set_config("ssl.ca.location", broker_certificate_location_.c_str());
        set_config("ssl.certificate.location", client_certificate_location_.c_str());
        set_config("ssl.key.location", key_location_.c_str());
        set_config("sasl.mechanisms", "SCRAM-SHA-256");
        set_config("sasl.username", client_name_.c_str());
        set_config("sasl.password", client_password_.c_str());
        set_config("api.version.request", "true");
    }
};

template <typename TLogger>
class LogDeliveryReportCb : public RdKafka::DeliveryReportCb {
    TLogger logger_;
public:
    void dr_cb (RdKafka::Message &message) {
        //std::cout << "Message delivery for (" << message.len() << " bytes): " <<
        //          message.errstr() << std::endl;
        //if (message.key())
        //    std::cout << "Key: " << *(message.key()) << ";" << std::endl;
        if (message.err()) {
            std::stringstream ss;
            ss << "kafka produce error:" << message.errstr();
            logger_.error(ss.str().c_str());
        }
    }

};


/* Use of this partitioner is pretty pointless since no key is provided
 * in the produce() call. */
class HashPartitionerCb : public RdKafka::PartitionerCb {
public:
    int32_t partitioner_cb (const RdKafka::Topic */*topic*/, const std::string *key,
                            int32_t partition_cnt, void */*msg_opaque*/) {
        if (key) {
            return djb_hash(key->c_str(), key->size()) % partition_cnt;
        } else {
            return 0;
        }
    }
private:

    static inline unsigned int djb_hash (const char *str, size_t len) {
        unsigned int hash = 5381;
        for (size_t i = 0 ; i < len ; i++) {
            hash = ((hash << 5) + hash) + str[i];
        }
        return hash;
    }
};

template <typename TLogger=StdLogger, typename TDeliveryReportCb=LogDeliveryReportCb<TLogger> >
class Producer {
public:
    explicit Producer(
            const char* brokers,
            const char* topic,
            const char* broker_certificate_location = 0,
            const char* client_certificate_location = 0,
            const char* client_key = 0,
            const char* client_name = 0,
            const char* client_password = 0)
            : brokers_(brokers), topic_name_(topic) {
        if (strcmp(brokers, "") == 0 || strcmp(topic, "") == 0) {
            // create dummy producer
            return;
        }
        std::string errstr;
        if (g_conf_->set("metadata.broker.list", brokers_, errstr) !=
            RdKafka::Conf::CONF_OK) {
            throw std::runtime_error("Kafka producer set metadata.broker.list failed: " + errstr);
        }
        //"socket.keepalive.enable=true"
        if (conf_->set("partitioner_cb", &hash_partitioner_, errstr) !=
            RdKafka::Conf::CONF_OK) {
            throw std::runtime_error("Kafka producer partitioner initialization failed: " + errstr);
        }

        if (g_conf_->set("dr_cb", &ex_dr_cb_, errstr) != ::RdKafka::Conf::CONF_OK) {
            throw std::runtime_error("Kafka producer failed to set delivery report callback:" + errstr);
        }
        if (broker_certificate_location != nullptr || strcmp(broker_certificate_location, "") != 0) {
            kafka_ssl_ = std::make_unique<KafkaSsl>(*g_conf_, broker_certificate_location, client_certificate_location, client_key, client_name, client_password);
        }

        producer_.reset(RdKafka::Producer::create(g_conf_.get(), errstr));
        if (!producer_) {
            throw std::runtime_error("Kafka producer failed to create producer:" + errstr);
        }

        topic_.reset(RdKafka::Topic::create(producer_.get(), topic_name_,
            conf_.get(), errstr));
        if (!topic_) {
            throw std::runtime_error("Kafka producer failed to create topic:" + errstr);
        }
    }

    bool produce(const char* const line, const size_t line_len, const char* const key = nullptr, size_t key_len = 0) {
        if (!producer_) {
            //dummy producer
            logger_.info(std::string("skip produce message:") + line);
            return true;
        } else {
            logger_.debug(std::string("produce key: ") + key + " message:" + line);
        }
        // call dr_cbs in current thread if need
        producer_->poll(0);
        if (key != nullptr && key_len == 0) {
            key_len = std::strlen(key) + 1;
        }
        const RdKafka::ErrorCode resp =
                producer_->produce(topic_.get(), partition_,
                                  RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
                                  const_cast<char*>(line), line_len,
                                  key, key_len,
                                   nullptr);
        if (resp != RdKafka::ERR_NO_ERROR) {
            last_error_ = RdKafka::err2str(resp);
            return false;
        }
        return true;
    }

    bool produce(const std::string& line, const std::string& key = "") {
        return produce(line.c_str(), line.size() + 1, key.c_str(), key.size() + 1); // null terminated
    }

    const std::string& error_string() const {
        return last_error_;
    }

    virtual ~Producer() {
        int count_down_timer = 10;
        while (producer_ && count_down_timer > 0 && producer_->outq_len() > 0) {
            std::stringstream ss;
            ss << "~Producer: Waiting for " << producer_->outq_len();
            logger_.error(ss.str().c_str());
            producer_->poll(1000);
            count_down_timer--;
        }
    }

    void poll(int ms = 0) {
        if (producer_) {
            producer_->poll(ms);
        }
    }

private:
    const std::string brokers_;
    const std::string topic_name_;

    const int32_t partition_ = RdKafka::Topic::PARTITION_UA;
    ::Labs::Kafka::HashPartitionerCb hash_partitioner_;
    TDeliveryReportCb ex_dr_cb_;
    TLogger logger_;

    std::string last_error_;
    std::unique_ptr<RdKafka::Conf> g_conf_ = std::unique_ptr<RdKafka::Conf>{RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)};
    std::unique_ptr<RdKafka::Conf> conf_ = std::unique_ptr<RdKafka::Conf>{RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC)};
    std::unique_ptr<KafkaSsl> kafka_ssl_;

    // deinitializing order important
    std::unique_ptr<RdKafka::Producer> producer_;
    std::unique_ptr<RdKafka::Topic> topic_;
};



template <typename TLogger=StdLogger>
class Consumer {
public:
    explicit Consumer(
            const char* brokers,
            const std::vector<std::string>& topics,
            const char* group,
            const char* broker_certificate_location = 0,
            const char* client_certificate_location = 0,
            const char* client_key = 0,
            const char* client_name = 0,
            const char* client_password = 0)
            : brokers_(brokers)
            , group_(group) {
        std::string errstr;
        if (g_conf_->set("metadata.broker.list", brokers_, errstr) != ::RdKafka::Conf::CONF_OK) {
            throw std::runtime_error("Kafka consumer failed to set metadata.broker.list:" + errstr);
        }

        if (g_conf_->set("group.id",  group, errstr) != ::RdKafka::Conf::CONF_OK) {
            throw std::runtime_error("Kafka consumer failed to set group.id:" + errstr);
        }

        if (g_conf_->set("default_topic_conf", conf_.get(), errstr) != ::RdKafka::Conf::CONF_OK) {
            throw std::runtime_error("Kafka consumer failed to set default_topic_conf:" + errstr);
        }
        if (g_conf_->set("session.timeout.ms", "30000", errstr) != ::RdKafka::Conf::CONF_OK) {
            throw std::runtime_error("Kafka consumer failed to set session.timeout.ms:" + errstr);
        }

        if (broker_certificate_location != nullptr) {
            kafka_ssl_ = std::make_unique<KafkaSsl>(*g_conf_, broker_certificate_location, client_certificate_location, client_key, client_name, client_password);
        }

        consumer_.reset(::RdKafka::KafkaConsumer::create(g_conf_.get(), errstr));
        if (!consumer_) {
            throw std::runtime_error("Kafka consumer failed to create consumer:" + errstr);
        }

        if (auto err = consumer_->subscribe(topics)) {
            throw std::runtime_error("Kafka consumer failed to subscribe topics:" + ::RdKafka::err2str(err));
        }
    }

    bool consume(
            const std::function<void(const char*, size_t, const char*)>& processor,
            const size_t max_msgs = 0,
            const int timeout_ms = 1,
            const std::function<bool()>& should_run = []() -> bool { return true; }) {
        size_t count = 0;
        while (should_run() && (max_msgs == 0 || count < max_msgs)) {
            std::unique_ptr<::RdKafka::Message> msg(consumer_->consume(timeout_ms));
            if (msg->err() == ::RdKafka::ERR_NO_ERROR) {
                processor(static_cast<const char *>(msg->payload()), msg->len(),
                          (msg->key() ? msg->key()->c_str() : nullptr));
                ++count;
            } else if (msg->err() == ::RdKafka::ERR__PARTITION_EOF || msg->err() == ::RdKafka::ERR__TIMED_OUT) {
                continue;
            } else {
                process_error(msg.get(), nullptr);
                return false;
            }
        }
        return true;
    }

    const std::string& error_string() const {
        return last_error_;
    }

    virtual ~Consumer() {
        if (consumer_) {
            consumer_->close();
        }
        ::RdKafka::wait_destroyed(10);
    }

private:
    void process_error(::RdKafka::Message* message, void* /*opaque*/) {
        switch (message->err()) {
            case ::RdKafka::ERR__TIMED_OUT:
            case ::RdKafka::ERR_NO_ERROR:
            case ::RdKafka::ERR__PARTITION_EOF:
            case ::RdKafka::ERR__UNKNOWN_TOPIC:
            case ::RdKafka::ERR__UNKNOWN_PARTITION:
            default:
                /* Errors */
                last_error_ = std::string("Consume failed: ") + message->errstr();
                break;
        }
    }



private:
    const std::string brokers_;
    const std::string topic_name_;
    const std::string group_;

    const int32_t partition_ = RdKafka::Topic::PARTITION_UA;
    TLogger logger_;

    std::string last_error_;
    std::unique_ptr<::RdKafka::Conf> g_conf_ = std::unique_ptr<RdKafka::Conf>{::RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)};
    std::unique_ptr<::RdKafka::Conf> conf_ = std::unique_ptr<RdKafka::Conf>{::RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC)};
    std::unique_ptr<KafkaSsl> kafka_ssl_;

    std::unique_ptr<::RdKafka::KafkaConsumer> consumer_;
};

}   //  namespace Kafka

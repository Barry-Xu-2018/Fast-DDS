// Copyright 2016 Proyectos y Sistemas de Mantenimiento SL (eProsima).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * @file HelloWorldPublisher.cpp
 *
 */

#include "HelloWorldPublisher.h"
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>

using namespace eprosima::fastdds::dds;

HelloWorldPublisher::HelloWorldPublisher()
    : participant_(nullptr)
    , publisher_(nullptr)
    , type_(new HelloWorldPubSubType())
{
}

bool HelloWorldPublisher::init()
{
    hello_.index(0);
    hello_.message("HelloWorld");

    //CREATE THE PARTICIPANT
    DomainParticipantQos pqos;
    pqos.properties().properties().emplace_back("dds.sec.auth.plugin",
            "builtin.PKI-DH");
    pqos.properties().properties().emplace_back("dds.sec.auth.builtin.PKI-DH.identity_ca",
            "file://certs/maincacert.pem");
    pqos.properties().properties().emplace_back("dds.sec.auth.builtin.PKI-DH.identity_certificate",
            "file://certs/mainpubcert.pem");
    pqos.properties().properties().emplace_back("dds.sec.auth.builtin.PKI-DH.private_key",
            "file://certs/mainpubkey.pem");
    pqos.properties().properties().emplace_back("dds.sec.access.plugin",
            "builtin.Access-Permissions");
    pqos.properties().properties().emplace_back("dds.sec.access.builtin.Access-Permissions.permissions_ca",
            "file://certs/maincacert.pem");
    pqos.properties().properties().emplace_back("dds.sec.access.builtin.Access-Permissions.governance",
            "file://certs/governance.smime");
    pqos.properties().properties().emplace_back("dds.sec.access.builtin.Access-Permissions.permissions",
            "file://certs/permissions.smime");
    pqos.properties().properties().emplace_back("dds.sec.crypto.plugin",
            "builtin.AES-GCM-GMAC");

    participant_ = DomainParticipantFactory::get_instance()->create_participant(0, pqos);

    if (participant_ == nullptr)
    {
        return false;
    }

    //REGISTER THE TYPE
    type_.register_type(participant_);

    //CREATE THE PUBLISHER
    publisher_ = participant_->create_publisher(PUBLISHER_QOS_DEFAULT);

    if (publisher_ == nullptr)
    {
        return false;
    }

    //CREATE THE TOPIC
    for (int i = 0; i < 20; ++i) {
        topic_[i] = participant_->create_topic("HelloWorldTopic_" + std::to_string(i), "HelloWorld", TOPIC_QOS_DEFAULT);
        if (topic_[i] == nullptr)
        {
            return false;
        }
    }

    //CREATE THE DATAWRITER
    DataWriterQos wqos;
    wqos.reliability().kind = RELIABLE_RELIABILITY_QOS;
    wqos.history().kind = KEEP_LAST_HISTORY_QOS;
    wqos.history().depth = 20;
    wqos.resource_limits().max_samples = 50;
    wqos.resource_limits().max_samples_per_instance = 20;
    wqos.reliable_writer_qos().times.heartbeatPeriod.seconds = 2;
    wqos.reliable_writer_qos().times.heartbeatPeriod.nanosec = 200 * 1000 * 1000;

    for (int i = 0; i < 20; ++i) {
        writer_[i] = publisher_->create_datawriter(topic_[i], wqos, &listener_);

        if (writer_[i] == nullptr)
        {
            return false;
        }
    }

    return true;

}

HelloWorldPublisher::~HelloWorldPublisher()
{
    for (int i = 0; i < 20; ++i) {
        if (writer_[i] != nullptr)
        {
            publisher_->delete_datawriter(writer_[i]);
        }
    }
    if (publisher_ != nullptr)
    {
        participant_->delete_publisher(publisher_);
    }
    for (int i = 0; i < 20; ++i) {
        if (topic_[i] != nullptr)
        {
            participant_->delete_topic(topic_[i]);
        }
    }
    DomainParticipantFactory::get_instance()->delete_participant(participant_);
}

void HelloWorldPublisher::PubListener::on_publication_matched(
        eprosima::fastdds::dds::DataWriter*,
        const eprosima::fastdds::dds::PublicationMatchedStatus& info)
{
    if (info.current_count_change == 1)
    {
        matched_ = info.total_count;
        first_connected_ = true;
        std::cout << "Publisher matched." << std::endl;
    }
    else if (info.current_count_change == -1)
    {
        matched_ = info.total_count;
        std::cout << "Publisher unmatched." << std::endl;
    }
    else
    {
        std::cout << info.current_count_change
                  << " is not a valid value for PublicationMatchedStatus current count change" << std::endl;
    }
}

void HelloWorldPublisher::run(
        uint32_t samples)
{
    for (uint32_t i = 0; i < samples; ++i)
    {
        if (!publish())
        {
            --i;
        }
        else
        {
            std::cout << "Message: " << hello_.message() << " with index: " << hello_.index() << " SENT" << std::endl;
        }
    }
}

bool HelloWorldPublisher::publish()
{
    if (listener_.matched_ > 0 || listener_.first_connected_)
    {
        hello_.index(hello_.index() + 1);
        writer_[0]->write(&hello_);
        return true;
    }
    return false;
}

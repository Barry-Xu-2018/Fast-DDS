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
 * @file HelloWorldSubscriber.cpp
 *
 */

#include "HelloWorldSubscriber.h"
#include <fastrtps/attributes/ParticipantAttributes.h>
#include <fastrtps/attributes/SubscriberAttributes.h>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>
#include <fastdds/dds/subscriber/qos/DataReaderQos.hpp>

using namespace eprosima::fastdds::dds;

HelloWorldSubscriber::HelloWorldSubscriber()
    : type_(new HelloWorldPubSubType())
{
}

bool HelloWorldSubscriber::init(
    size_t subside_participant_count,
    size_t subside_subscriber_count,
    size_t subside_topic_count,
    size_t subside_reader_count
)
{
    if (subside_participant_count < 1 ||
        subside_subscriber_count < 1 ||
        subside_topic_count < 1 ||
        subside_reader_count < 1) {
        std::cout << "HelloWorldSubscriber::init parameter error" << std::endl;
        return false;
    }

    DomainParticipantQos pqos;

    for (size_t i = 0; i < subside_participant_count; ++i) {
        pqos.name("Participant_sub_" + std::to_string(i));
        auto participant_ = DomainParticipantFactory::get_instance()->create_participant(0, pqos);

        if (participant_ == nullptr)
        {
            return false;
        }

        participant_list_.push_back(participant_);

        //REGISTER THE TYPE
        type_.register_type(participant_);
    }

    for (size_t i = 0; i < subside_subscriber_count; ++i) {
        size_t j = i;
        if (j >= participant_list_.size()) {
            j = participant_list_.size()-1;
        }
        //CREATE THE SUBSCRIBER
        auto subscriber_ = participant_list_[j]->create_subscriber(SUBSCRIBER_QOS_DEFAULT, nullptr);

        if (subscriber_ == nullptr)
        {
            return false;
        }

        subscriber_list_.push_back(subscriber_);
    }

    for (size_t i = 0; i < subside_topic_count; ++i) {
        size_t j = i;
        if (j >= participant_list_.size()) {
            j = participant_list_.size()-1;
        }

        std::string topic_name = "HelloWorldTopic" + std::to_string(i);
        auto topic_ = participant_list_[j]->create_topic(topic_name, "HelloWorld", TOPIC_QOS_DEFAULT);

        if (topic_ == nullptr)
        {
            return false;
        }

        topic_list_.push_back(topic_);
    }

    // CREATE THE READER
    DataReaderQos rqos = DATAREADER_QOS_DEFAULT;
    rqos.reliability().kind = BEST_EFFORT_RELIABILITY_QOS;
    rqos.durability().kind = VOLATILE_DURABILITY_QOS;
    rqos.data_sharing().automatic();

    for (size_t i = 0; i < subside_reader_count; ++i) {
        size_t j = i;
        if (j >= subscriber_list_.size()) {
            j = subscriber_list_.size()-1;
        }

        size_t k = i;
        if (k >= topic_list_.size()) {
            k = topic_list_.size()-1;
        }

        auto reader_ = subscriber_list_[j]->create_datareader(topic_list_[k], rqos, &listener_);

        if (reader_ == nullptr)
        {
            return false;
        }

        reader_list_.push_back(reader_);
    }

    return true;
}

HelloWorldSubscriber::~HelloWorldSubscriber()
{
    for (size_t i = 0; i < reader_list_.size(); ++i) {
        size_t j = i;
        if (j >= subscriber_list_.size()) {
            j = subscriber_list_.size()-1;
        }
        subscriber_list_[j]->delete_datareader(reader_list_[i]);
    }

    for (size_t i = 0; i < subscriber_list_.size(); ++i) {
        size_t j = i;
        if (j >= participant_list_.size()) {
            j = participant_list_.size()-1;
        }
        participant_list_[j]->delete_subscriber(subscriber_list_[i]);
    }

    for (size_t i = 0; i < topic_list_.size(); ++i) {
        size_t j = i;
        if (j >= participant_list_.size()) {
            j = participant_list_.size()-1;
        }
        participant_list_[j]->delete_topic(topic_list_[i]);
    }

    for (size_t i = 0; i < participant_list_.size(); ++i) {
        DomainParticipantFactory::get_instance()->delete_participant(participant_list_[i]);
    }
}

void HelloWorldSubscriber::SubListener::on_subscription_matched(
        DataReader*,
        const SubscriptionMatchedStatus& info)
{
    if (info.current_count_change == 1)
    {
        matched_ = info.total_count;
        std::cout << "Subscriber: a writer discovered." << std::endl;
    }
    else if (info.current_count_change == -1)
    {
        matched_ = info.total_count;
        std::cout << "Subscriber: a writer disappeared." << std::endl;
    }
    else
    {
        std::cout << info.current_count_change
                  << " is not a valid value for SubscriptionMatchedStatus current count change" << std::endl;
    }
}

void HelloWorldSubscriber::SubListener::on_data_available(
        DataReader* reader)
{
    SampleInfo info;
    if (reader->take_next_sample(&hello_, &info) == ReturnCode_t::RETCODE_OK)
    {
        if (info.instance_state == ALIVE_INSTANCE_STATE)
        {
            samples_++;
            // Print your structure data here.
            std::cout << "Message " << hello_.message() << " " << hello_.index() << " RECEIVED" << std::endl;
        }
    }
}

void HelloWorldSubscriber::run()
{
    std::cout << "Subscriber running. Please press enter to stop the Subscriber" << std::endl;
    std::cin.ignore();
}

void HelloWorldSubscriber::run(
        uint32_t number)
{
    std::cout << "Subscriber running until " << number << "samples have been received" << std::endl;
    while (number > listener_.samples_)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
}

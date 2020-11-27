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
#include <fastrtps/attributes/ParticipantAttributes.h>
#include <fastrtps/attributes/PublisherAttributes.h>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/publisher/qos/PublisherQos.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/qos/DataWriterQos.hpp>

#include <thread>

using namespace eprosima::fastdds::dds;

HelloWorldPublisher::HelloWorldPublisher()
    : type_(new HelloWorldPubSubType())
{
}

bool HelloWorldPublisher::init(
    size_t pubside_participant_count,
    size_t pubside_publisher_count,
    size_t pubside_topic_count,
    size_t pubside_writer_count
)
{
    if (pubside_participant_count < 1 ||
        pubside_publisher_count < 1 ||
        pubside_topic_count < 1 ||
        pubside_writer_count < 1) {
        std::cout << "HelloWorldPublisher::init parameter error" << std::endl;
        return false;
    }

    hello_.index(0);
    hello_.message("HelloWorld");
    DomainParticipantQos pqos;

    for (size_t i = 0; i < pubside_participant_count; ++i) {
        pqos.name("Participant_pub_" + std::to_string(i));
        auto participant_ = DomainParticipantFactory::get_instance()->create_participant(0, pqos);

        if (participant_ == nullptr)
        {
            return false;
        }

        participant_list_.push_back(participant_);

        //REGISTER THE TYPE
        type_.register_type(participant_);
    }

    for (size_t i = 0; i < pubside_publisher_count; ++i) {
        size_t j = i;
        if (j >= participant_list_.size()) {
            j = participant_list_.size()-1;
        }
        //CREATE THE PUBLISHER
        auto publisher_ = participant_list_[j]->create_publisher(PUBLISHER_QOS_DEFAULT, nullptr);

        if (publisher_ == nullptr)
        {
            return false;
        }

        publisher_list_.push_back(publisher_);
    }

    for (size_t i = 0; i < pubside_topic_count; ++i) {
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

    DataWriterQos wqos = DATAWRITER_QOS_DEFAULT;
    wqos.reliability().kind = BEST_EFFORT_RELIABILITY_QOS;
    wqos.history().depth = 10;
    wqos.data_sharing().automatic();

    for (size_t i = 0; i < pubside_writer_count; ++i) {
        // CREATE THE WRITER
        size_t j = i;
        if (j >= publisher_list_.size()) {
            j = publisher_list_.size()-1;
        }

        size_t k = i;
        if (k >= topic_list_.size()) {
            k = topic_list_.size()-1;
        }

        auto writer_ = publisher_list_[j]->create_datawriter(topic_list_[k], wqos, &listener_);

        if (writer_ == nullptr)
        {
            return false;
        }

        writer_list_.push_back(writer_);
    }

    return true;
}

HelloWorldPublisher::~HelloWorldPublisher()
{
    for (size_t i = 0; i < writer_list_.size(); ++i) {
        size_t j = i;
        if (j >= publisher_list_.size()) {
            j = publisher_list_.size()-1;
        }
        publisher_list_[j]->delete_datawriter(writer_list_[i]);
    }

    for (size_t i = 0; i < publisher_list_.size(); ++i) {
        size_t j = i;
        if (j >= participant_list_.size()) {
            j = participant_list_.size()-1;
        }
        participant_list_[j]->delete_publisher(publisher_list_[i]);
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

void HelloWorldPublisher::PubListener::on_publication_matched(
        eprosima::fastdds::dds::DataWriter*,
        const eprosima::fastdds::dds::PublicationMatchedStatus& info)
{
    if (info.current_count_change == 1)
    {
        matched_ = info.total_count;
        firstConnected_ = true;
        std::cout << "Publisher: a reader discovered." << std::endl;
    }
    else if (info.current_count_change == -1)
    {
        matched_ = info.total_count;
        std::cout << "Publisher: a reader disappeared." << std::endl;
    }
    else
    {
        std::cout << info.current_count_change
                  << " is not a valid value for PublicationMatchedStatus current count change" << std::endl;
    }
}

void HelloWorldPublisher::runThread(
        uint32_t samples,
        uint32_t sleep)
{
    if (samples == 0)
    {
        while (!stop_)
        {
            if (publish(false))
            {
                std::cout << "Message: " << hello_.message() << " with index: " << hello_.index()
                          << " SENT" << std::endl;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep));
        }
    }
    else
    {
        for (uint32_t i = 0; i < samples; ++i)
        {
            if (!publish())
            {
                --i;
            }
            else
            {
                std::cout << "Message: " << hello_.message() << " with index: " << hello_.index()
                          << " SENT" << std::endl;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep));
        }
    }
}

void HelloWorldPublisher::run(
        uint32_t samples,
        uint32_t sleep)
{
    stop_ = false;
    std::thread thread(&HelloWorldPublisher::runThread, this, samples, sleep);
    if (samples == 0)
    {
        std::cout << "Publisher running. Please press enter to stop the Publisher at any time." << std::endl;
        std::cin.ignore();
        stop_ = true;
    }
    else
    {
        std::cout << "Publisher running " << samples << " samples." << std::endl;
    }
    thread.join();
}

bool HelloWorldPublisher::publish(
        bool waitForListener)
{
    if (listener_.firstConnected_ || !waitForListener || listener_.matched_ > 0)
    {
        hello_.index(hello_.index() + 1);
        for (auto writer_: writer_list_) {
            writer_->write(&hello_);
        }
        return true;
    }
    return false;
}

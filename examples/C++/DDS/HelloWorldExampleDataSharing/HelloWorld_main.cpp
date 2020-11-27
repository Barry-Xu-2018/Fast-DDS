// Copyright 2019 Proyectos y Sistemas de Mantenimiento SL (eProsima).
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
 * @file HelloWorld_main.cpp
 *
 */

#include "HelloWorldPublisher.h"
#include "HelloWorldSubscriber.h"

#include <fastdds/dds/domain/DomainParticipantFactory.hpp>

#include <fastrtps/log/Log.h>
#include <thread>

#include <boost/program_options.hpp>
using namespace boost::program_options;

using eprosima::fastdds::dds::Log;

int main(
    int argc,
    char **argv)
{
    //    Log::SetVerbosity(Log::Kind::Info);
    //    Log::SetCategoryFilter(std::regex("(RTPS_WRITER)|(RTPS_READER)"));
    options_description description("Datasharing test tool command line interfaces");
    description.add_options()("help,h", "help message to show interfaces")
        ("type,t", value<std::string>()->default_value("publisher"), "application type, e.g. publisher, subscriber, all")
        ("count,c", value<int>()->default_value(10), "count to send sample data")
        ("sleep,s", value<int>()->default_value(100), "time period between two sending data")
        ("pubside_participant_count", value<size_t>()->default_value(1), "participant count on publisher side")
        ("pubside_publisher_count", value<size_t>()->default_value(1), "publisher count on publisher side")
        ("pubside_topic_count", value<size_t>()->default_value(1), "topic count on publisher side")
        ("pubside_writer_count", value<size_t>()->default_value(1), "writer count on publisher side")
        ("subside_participant_count", value<size_t>()->default_value(1), "participant count on subscriber side")
        ("subside_subscriber_count", value<size_t>()->default_value(1), "subscriber count on subscriber side")
        ("subside_topic_count", value<size_t>()->default_value(1), "topic count on subscriber side")
        ("subside_reader_count", value<size_t>()->default_value(1), "reader count on subscriber side")
        ("debug,d", value<bool>()->default_value(false), "open debug mode");

    variables_map vm;
    try {
        store(parse_command_line(argc, argv, description), vm);
    } catch (const boost::program_options::error_with_option_name& e) {
        std::cout << e.what() << std::endl;
        std::cout << description << std::endl;
        return 1;
    } catch (...) {
        std::cout << "unexpected exception caught" << std::endl;
        return 1;
    }
    notify(vm);

    if (vm.count("help"))
    {
        std::cout << description << std::endl;
        return 0;
    }

    std::string type = vm["type"].as<std::string>();
    int count = vm["count"].as<int>();
    int sleep = vm["sleep"].as<int>();
    size_t pubside_participant_count = vm["pubside_participant_count"].as<size_t>();
    size_t pubside_publisher_count = vm["pubside_publisher_count"].as<size_t>();
    size_t pubside_topic_count = vm["pubside_topic_count"].as<size_t>();
    size_t pubside_writer_count = vm["pubside_writer_count"].as<size_t>();
    size_t subside_participant_count = vm["subside_participant_count"].as<size_t>();
    size_t subside_subscriber_count = vm["subside_subscriber_count"].as<size_t>();
    size_t subside_topic_count = vm["subside_topic_count"].as<size_t>();
    size_t subside_reader_count = vm["subside_reader_count"].as<size_t>();
    bool debug = vm["debug"].as<bool>();

    if (debug) {
        Log::SetVerbosity(Log::Kind::Info);
        Log::SetCategoryFilter(std::regex("(RTPS_WRITER)|(RTPS_READER)"));
    }

    std::cout << "Starting " << std::endl;

    if (type == "publisher")
    {
        HelloWorldPublisher mypub;
        if (mypub.init(
            pubside_participant_count,
            pubside_publisher_count,
            pubside_topic_count,
            pubside_writer_count
            ))
        {
            mypub.run(static_cast<uint32_t>(count), static_cast<uint32_t>(sleep));
        }
    }
    else if (type == "subscriber")
    {
        HelloWorldSubscriber mysub;
        if (mysub.init(
            subside_participant_count,
            subside_subscriber_count,
            subside_topic_count,
            subside_reader_count
        ))
        {
            mysub.run();
        }
    }
    else if (type == "all")
    {
        HelloWorldPublisher mypub;
        std::unique_ptr<std::thread> pub_thread;
        if (mypub.init(
            pubside_participant_count,
            pubside_publisher_count,
            pubside_topic_count,
            pubside_writer_count
        ))
        {
            pub_thread.reset(new std::thread(
                [&mypub, &count, &sleep]() {
                    mypub.run(static_cast<uint32_t>(count), static_cast<uint32_t>(sleep));
                }));
        }

        HelloWorldSubscriber mysub;
        std::unique_ptr<std::thread> sub_thread;
        if (mysub.init(
            subside_participant_count,
            subside_subscriber_count,
            subside_topic_count,
            subside_reader_count
        ))
        {
            // mysub.run();
            sub_thread.reset(new std::thread(
                [&mysub]() {
                    mysub.run();
                }));
        }

        pub_thread->join();
        sub_thread->join();
    }
    else
    {
        std::cout << "publisher OR subscriber argument needed" << std::endl;
    }

    if (debug) {
        Log::Reset();
    }
    return 0;
}

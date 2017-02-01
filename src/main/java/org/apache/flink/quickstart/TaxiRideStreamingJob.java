package org.apache.flink.quickstart;


/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.*;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.*;



/**
 This class creates stream from input text file of taxi journets and filters out journeys that are not in New York

 @author Darren Gilligan

 */
public class TaxiRideStreamingJob {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // configure event-time processing
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

// get the taxi ride data stream
        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource("/Users/darrengilligan1/Documents/flink_training/flink-java-project/data/nycTaxiRides.gz", 0, 2));

        DataStream<TaxiRide> filtered = rides.filter(new FilterFunction<TaxiRide>() {
            @Override
            public boolean filter(TaxiRide theTaxiRide) {
                return GeoUtils.isInNYC(theTaxiRide.startLon, theTaxiRide.startLat) && GeoUtils.isInNYC(theTaxiRide.endLon, theTaxiRide.endLat);
            }
        });

        filtered.print();


        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

}

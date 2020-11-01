package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class DistrictCountMapperTest {

    @Mock
    private Mapper.Context context;
    private DistrictCountMapper districtCountMapper;
    String csvFile = "src/test/resources/data/trees.csv";

    @Before
    public void setup() { this.districtCountMapper = new DistrictCountMapper(); }

    @Test
    public void testMap() throws IOException, InterruptedException {
        BufferedReader br = null;
        String line = "";
        try {
            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
                this.districtCountMapper.map(null, new Text(line), this.context);
            }
        } catch (IOException e) {
        e.printStackTrace();
    }
    verify(this.context, times(36))
        .write(new Text("16"), new IntWritable(1));
    }
}

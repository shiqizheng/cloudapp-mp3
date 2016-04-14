import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext context;


  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {
        file= "/cloudapp-mp3/data.txt";
        BufferedReader buffIn=new BufferedReader(new FileReader(file));
     /*
    ----------------------TODO-----------------------
    Task: initialize the file reader
        file= "/cloudapp-mp3/data.txt"
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

    ------------------------------------------------- */

    this.context = context;
    this._collector = collector;
  }
  @Override
  public void nextTuple() {

  Utils.sleep(100);
  string line = buffIn.readLine();
  _collector.emit(new Values(line));

     /*
    ----------------------TODO-----------------------
    Task:
    1. read the next line and emit a tuple for it
    2. don't forget to sleep when the file is entirely read to prevent a busy-loop

    ------------------------------------------------- */


  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("word"));

  }

  @Override
  public void close() {
   /*
    ----------------------TODO-----------------------
    Task: close the file


    ------------------------------------------------- */
      buffIn.close()
  }


  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}



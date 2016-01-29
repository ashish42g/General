package mapSideJoin;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

public class EmployeeMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static HashMap<String, String> deptDataMap = new HashMap<>();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        URI[] paths = context.getCacheFiles();
        for (URI path : paths) {
            if(FilenameUtils.getName(paths[0].toString()).equals("DeptDataMap.txt")){
                loadDeptfromCache(path.getPath());
            }
        }
    }

    private void loadDeptfromCache(String path) throws IOException {

        String lineRead;
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(path));

            while((lineRead = br.readLine()) != null){

                String[] deptDataFields = lineRead.toString().split(" ");
                deptDataMap.put(deptDataFields[0].trim(), deptDataFields[1].trim()+"\t"+deptDataFields[2].trim());
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            br.close();
        }
    }

    @Override
    protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

        String line = values.toString();
        String[] empFields = line.split(" ");
        String deptNameAndLoc = null;
        if(empFields != null && empFields.length == 3){
            deptNameAndLoc = deptDataMap.get(empFields[2]);
            if(deptNameAndLoc == null || deptNameAndLoc.equals(""))
                deptNameAndLoc = "NF";
        }

        context.write(new Text(""), new Text(empFields[0]+"\t"+empFields[1]+" "+empFields[2]+"\t"+deptNameAndLoc));
    }
}

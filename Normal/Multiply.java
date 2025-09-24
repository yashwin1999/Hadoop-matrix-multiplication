import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Elem implements Writable {
    short tag;
    int index;
    double value;

    Elem() {}
    Elem(short tag, int index, double value) {
        this.tag = tag;
        this.index = index;
        this.value = value;
    }

    public void write(DataOutput out) throws IOException {
        out.writeShort(tag);
        out.writeInt(index);
        out.writeDouble(value);
    }

    public void readFields(DataInput in) throws IOException {
        tag = in.readShort();
        index = in.readInt();
        value = in.readDouble();
    }
}

class Pair implements WritableComparable<Pair> {
    public int i;
    public int j;

    Pair() {}
    Pair(int i, int j) {
        this.i = i;
        this.j = j;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
    }

    public void readFields(DataInput in) throws IOException {
        i = in.readInt();
        j = in.readInt();
    }

    public int compareTo(Pair p) {
        if (i != p.i)
            return Integer.compare(i, p.i);
        return Integer.compare(j, p.j);
    }

    public String toString() {
        return i + "," + j;
    }
}

public class Multiply {
    public static class MapperM extends Mapper<Object, Text, IntWritable, Elem> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner S = new Scanner(value.toString()).useDelimiter(",");
            int i = S.nextInt();
            int j = S.nextInt();
            double v = S.nextDouble();
            context.write(new IntWritable(j), new Elem((short) 0, i, v));
            S.close();
        }
    }

    public static class MapperN extends Mapper<Object, Text, IntWritable, Elem> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner S = new Scanner(value.toString()).useDelimiter(",");
            int i = S.nextInt();
            int j = S.nextInt();
            double v = S.nextDouble();
            context.write(new IntWritable(i), new Elem((short) 1, j, v));
            S.close();
        }
    }

    public static class ReducerMN extends Reducer<IntWritable, Elem, Pair, DoubleWritable> {
        public void reduce(IntWritable key, Iterable<Elem> values, Context context) throws IOException, InterruptedException {
            List<Elem> A = new ArrayList<>();
            List<Elem> B = new ArrayList<>();

            for (Elem e : values) {
                Elem dup = new Elem(e.tag, e.index, e.value);
                if (e.tag == 0) {
                    A.add(dup);
                } else {
                    B.add(dup);
                }
            }

            for (Elem a : A) {
                for (Elem b : B) {
                    context.write(new Pair(a.index, b.index), new DoubleWritable(a.value * b.value));
                }
            }
        }
    }

    public static class MapperPair extends Mapper<Object, Text, Pair, DoubleWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length != 2) {
                System.err.println("Invalid input line: " + value.toString());
                return;
            }

            String[] indices = parts[0].split(",");
            if (indices.length != 2) {
                System.err.println("Invalid key format: " + parts[0]);
                return;
            }

            try {
                int i = Integer.parseInt(indices[0].trim());
                int j = Integer.parseInt(indices[1].trim());
                double v = Double.parseDouble(parts[1].trim());

                context.write(new Pair(i, j), new DoubleWritable(v));
            } catch (NumberFormatException e) {
            }
        }
    }


    public static class ReducerPair extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {
        public void reduce(Pair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable v : values) {
                sum += v.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

//        conf.set("mapreduce.input.fileinputformat.split.maxsize", "67108864");

        long startTime = System.currentTimeMillis();

        Job job1 = Job.getInstance(conf, "Multiply");
        job1.setJarByClass(Multiply.class);
        job1.setReducerClass(ReducerMN.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Elem.class);
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, MapperM.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, MapperN.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        boolean job1Success = job1.waitForCompletion(true);

        if (!job1Success) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "Sum");
        job2.setJarByClass(Multiply.class);
        job2.setMapperClass(MapperPair.class);
        job2.setReducerClass(ReducerPair.class);
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(DoubleWritable.class);
//        job2.setNumReduceTasks(4);

        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        boolean job2Success = job2.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println("Total Execution Time: " + (endTime - startTime) + " ms");

        System.exit(job2Success ? 0 : 1);
    }
}
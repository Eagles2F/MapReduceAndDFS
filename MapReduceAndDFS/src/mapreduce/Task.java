package mapreduce;
public abstract class Task{
    int taskId;
    TaskStatus taskStatus;
    private int type; // specify the job is a map or a reduce, USELESS NOW!!
    public static final int MAP = 0;
    public static final int REDUCE = 1; // USELESS NOW!!

    // should get from conf file
    @SuppressWarnings("rawtypes")
    private Class mapClass;
    @SuppressWarnings("rawtypes")
    private Class mapInputKeyClass;
    @SuppressWarnings("rawtypes")
    private Class mapInputValueClass;
    @SuppressWarnings({ "rawtypes", "unused" })
    private Class mapOutputKeyClass;
    @SuppressWarnings({ "rawtypes", "unused" })
    private Class mapOutputValueClass;

    @SuppressWarnings("rawtypes")
    private Class reduceClass;
    @SuppressWarnings("rawtypes")
    private Class reduceInputKeyClass;
    @SuppressWarnings({ "rawtypes", "unused" })
    private Class reduceInputValueClass;
    @SuppressWarnings({ "unused", "rawtypes" })
    private Class reduceOutputKeyClass;
    @SuppressWarnings({ "rawtypes", "unused" })
    private Class reduceOutputValueClass;
    /*
     * spliter
     * combiner
     */
}
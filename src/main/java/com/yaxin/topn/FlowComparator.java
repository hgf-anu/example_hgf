package com.yaxin.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 让所有数据分到同一组的比较器
 */
public class FlowComparator extends WritableComparator {
    protected FlowComparator() {
        super(FlowBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //mapper和reducer的数据形式期待不一样.如果想让所有数据放到同一组,如何写?
        //return 0 不管比较的两者是什么,都返回0.即相同,所有人都在同一组
        return 0;
    }
}

package org.menagerie.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: Apr 26, 2011
 *         Time: 9:46:02 AM
 */
public class TimingAccumulator {
    private List<Long> timings;

    public TimingAccumulator() {
        timings = Collections.synchronizedList(new LinkedList<Long>());
    }

    public void addTiming(long timing){
        timings.add(timing);
    }

    public void printResults(){
        if(timings.size()<=0){
            System.out.println("No Results");
            return;
        }
        List<Long> timingCopy = new ArrayList<Long>(timings);
        Collections.sort(timingCopy);
        //find the median
        long median = timingCopy.get(timingCopy.size()/2);

        long sum=0;
        for(long timing:timingCopy){
            sum+=timing;
        }
        float avg = ((float)sum)/timings.size();

        double squareDiffsSum=0;
        for(long timing:timingCopy){
            float diff = timing-avg;
            squareDiffsSum+=Math.pow(diff,2);
        }

        double stdDev = Math.sqrt(squareDiffsSum/timings.size());

        System.out.printf("Mean=%f\t Median=%d\t StdDev=%f%n",avg,median,stdDev);
    }
}

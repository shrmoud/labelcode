import java.util.Collections;
import java.util.Vector;
import java.util.concurrent.Callable;

public class LabelPropagationWorker implements Callable<Boolean>
{

	private Vector<Integer> dominantLabels;
	private Vector<Integer> labelCounts;
	private int nodeId;
	private int partitionSize = 100; //Number of Partition Needed from the Graph
	private Vector<Node> nodeList; 	// Shared
	private Vector<Integer> threshold; 	// Shared

	public LabelPropagationWorker(Vector<Node> nodeList, Vector<Integer> threshold) 
	{
		dominantLabels = new Vector<Integer>();
		labelCounts = new Vector<Integer>(nodeList.size());

		for (int i=0; i<nodeList.size(); i++) 
		{
			labelCounts.add(Integer.valueOf(0));
		}

		this.nodeList = nodeList;//Shared reference
		this.threshold = threshold;

		System.out.println("Worker created.");
	}



	public void setNodeToProcess(int nodeId) 
	{
		this.nodeId=nodeId;
	}

	@Override
	public Boolean call() 
	{

		if (nodeId==-1) 
		{
			return Boolean.FALSE;
		}

		boolean continueRunning = false;

		Collections.fill(labelCounts, Integer.valueOf(0));
		dominantLabels.clear();

		Node currentNode = nodeList.get(nodeId);
		int maxCount = 0;
		// Neighbors
		for (Integer neighborId : currentNode.getNeighbors()) 
		{

			int nLabel = nodeList.get(neighborId).getLabel();
			if (nLabel == 0)
				continue; // No label yet

			int nLabelCount = labelCounts.get(nLabel) + 1;
			labelCounts.set(nLabel, nLabelCount);

			if (maxCount < nLabelCount) 
			{
				maxCount = nLabelCount;
				//dominantLabels.clear();
			}
			dominantLabels.add(nLabel);
		}

		int dominantLabelIndex = -1, index = -1, count = 0;

		//	System.out.println(dominantLabels);
		//	System.out.println(labelCounts);

		if(dominantLabels.size() > 0 && labelCounts.size() > 0) 
		{
			dominantLabelIndex = Collections.max(labelCounts); //Value
			index = labelCounts.indexOf(dominantLabelIndex);
		}

		while(labelCounts.size() > 0 && dominantLabels.size() > 0)
		{
			//System.out.println("Looping");
			if(threshold.get(index) < partitionSize )
			{
				//System.out.println("Threshold < Size");
				int currentVal = currentNode.getLabel();		
				int nVal = threshold.get(index) + 1;
				threshold.set(index, nVal);
				int cVal = threshold.get(currentVal) - 1;
				threshold.set(currentVal, cVal);
				continueRunning = false;
				currentNode.setLabel(index);
				dominantLabels.clear();
				break;
			}
			else if(threshold.get(index) == partitionSize &&  currentNode.getLabel() == index )
			{
				//System.out.println("Threshold  = Size and Label == Index");
				int currentVal = currentNode.getLabel();
				continueRunning = false;
				dominantLabels.clear();
				break;
			}
			else if(threshold.get(index) == partitionSize &&  currentNode.getLabel() != index )
			{
				//System.out.println("Threshold  = Size and Label != Index");
				labelCounts.set(index,0);
				dominantLabelIndex = Collections.max(labelCounts); //Value
				index = labelCounts.indexOf(dominantLabelIndex);
				continueRunning = true;
				if(dominantLabelIndex == 1 && dominantLabels.size() > 0)
				{					
					index = Collections.min(dominantLabels);
					int i = dominantLabels.indexOf(index);
					count++;
					if(count>0)
					{
						dominantLabels.remove(i);
						if(dominantLabels.size() > 0)
						{
							index = Collections.min(dominantLabels);
						}
						else
						{
							continueRunning = false;
							break;
						}
					}
				}
			}
		}
		return Boolean.valueOf(continueRunning);
	}
}
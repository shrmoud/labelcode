import java.util.Collections;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.Callable;

public class LabelPropagationWorker implements Callable<Boolean>{

	private Vector<Integer> dominantLabels;
	private Vector<Integer> labelCounts;
	//private Random randGen;
	
	private int nodeId;
	private int partitionSize = 3; //Number of Partition Needed from the Graph

	// Shared
	private Vector<Node> nodeList;
	private Vector<Integer> threshold;

	public LabelPropagationWorker(Vector<Node> nodeList, Vector<Integer> threshold) 
	{
		dominantLabels = new Vector<Integer>();
		labelCounts = new Vector<Integer>(nodeList.size());
		
		for (int i=0; i<nodeList.size(); i++) 
		{
			labelCounts.add(Integer.valueOf(0));
		}
		
		//randGen = new Random();
		
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
				continue; // No label yet (only if initial labels are given?)

			int nLabelCount = labelCounts.get(nLabel) + 1;
			labelCounts.set(nLabel, nLabelCount);// Careful of wrapping
			// un-wrapping here!

			if (maxCount < nLabelCount) 
			{
				maxCount = nLabelCount;
				dominantLabels.clear();
				dominantLabels.add(nLabel);
			}
			else if (maxCount == nLabelCount) 
			{
				dominantLabels.add(nLabel);
			}
		
		}

//		if (dominantLabels.size() > 0) 
//		{
			// Randomly select from dominant labels
		    //	int rand = randGen.nextInt(dominantLabels.size());
				
				//rand = dominantLabels.get(rand);
		
			
			// Check if *current* label of node is also dominant
		
		
	/*	if (labelCounts.get(currentVal) != maxCount) 
		{
			// it's not. continue
			continueRunning = true;
	
		}	
	 */		
	
		while(dominantLabels.size() > 0)
		{
			int val = Collections.min(dominantLabels);
			int rand = dominantLabels.indexOf(val);
			if(threshold.get(rand) == partitionSize)
			{
				dominantLabels.remove(rand);
			}
			else				
			{
				int currentVal = currentNode.getLabel();		
				int nVal = threshold.get(rand) + 1;
				threshold.set(rand, nVal);
				int cVal = threshold.get(currentVal) - 1;
				threshold.set(currentVal, cVal);
				currentNode.setLabel(val);
				continueRunning = true;
				break;
			}
		}
		return Boolean.valueOf(continueRunning);
	}
}
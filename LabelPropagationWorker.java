import java.util.Collections;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.Callable;

public class LabelPropagationWorker implements Callable<Boolean>
{

	private Vector<Integer> dominantLabels;
	private Vector<Integer> labelCounts;
	//private Random randGen;

	private int nodeId;
	private int partitionSize = 50; //Number of Partition Needed from the Graph

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

	
	public boolean findMin(Node currentNode, int maxLabel, int maxCount)
	{
		int max = dominantLabels.indexOf(maxLabel);
		int currentVal = currentNode.getLabel();		
		int nVal = threshold.get(max) + 1;
		threshold.set(max, nVal);
		int cVal = threshold.get(currentVal) - 1;
		threshold.set(currentVal, cVal);
		currentNode.setLabel(max);
		dominantLabels.clear();
		if (labelCounts.get(currentVal) != maxCount) 
		{
			//System.out.println("If the current label is not a dominant label, then continue running");
			return true;
		}
		else
		{
			return false;
		}
		
	}

	
	@Override
	public Boolean call() 
	{

		if (nodeId==-1) 
		{
			return Boolean.FALSE;
		}


		int maxLabel = -1, maxLabelCount =-1;
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
			labelCounts.set(nLabel, nLabelCount);

			if (maxCount < nLabelCount) 
			{
				maxCount = nLabelCount;
				//dominantLabels.clear();
				dominantLabels.add(nLabel);
			}
			else if (maxCount == nLabelCount) 
			{
				dominantLabels.add(nLabel);
			}

		}

	//	System.out.println(dominantLabels);

		if(dominantLabels.size() > 0)
		{
			maxLabel = dominantLabels.get(0);
			maxLabelCount = labelCounts.get(maxLabel);

			for(int i=1; i<dominantLabels.size();i++)
			{
				int newMaxLabel = dominantLabels.get(i);
				int newMaxLabelCount = labelCounts.get(maxLabel);

				if(newMaxLabelCount > maxLabelCount)
				{
					maxLabel = newMaxLabel;
					maxLabelCount = newMaxLabelCount;
				}	 
			}
			//System.out.println("I found maxLabel as " + maxLabel);
		}

		
		if(dominantLabels.size() > 0 && maxLabel != dominantLabels.get(0))
		{
			int max = dominantLabels.indexOf(maxLabel);
			if(threshold.get(max) < partitionSize)
			{
				//continueRunning = findMin(currentNode, max, maxCount);
				//System.out.println("Removing Label which exceeds Threshold");
				int currentVal = currentNode.getLabel();		
				int nVal = threshold.get(max) + 1;
				threshold.set(max, nVal);
				int cVal = threshold.get(currentVal) - 1;
				threshold.set(currentVal, cVal);
				if (labelCounts.get(currentVal) != maxCount) 
				{
					//System.out.println("If the current label is not a dominant label, then continue running");
					continueRunning = true;
				}
				currentNode.setLabel(max);
				dominantLabels.clear();
			}
			else if(threshold.get(max) == partitionSize)
			{
				int currentVal = currentNode.getLabel();
				if (labelCounts.get(currentVal) != maxCount) 
				{
					//System.out.println("If the current label is not a dominant label, then continue running");
					continueRunning = true;
				}
				dominantLabels.clear();
			}
			else
			{
				while(dominantLabels.size() > 0)
				{
					int val = Collections.min(dominantLabels);
					int valIndex = dominantLabels.indexOf(val);
					if(threshold.get(val) == partitionSize)
					{
						//System.out.println("Removing Label which exceeds Threshold");
						dominantLabels.remove(valIndex);
						continue;
					}
					else				
					{
						//System.out.println("In Else");
						int currentVal = currentNode.getLabel();		
						int nVal = threshold.get(valIndex) + 1;
						threshold.set(valIndex, nVal);
						int cVal = threshold.get(currentVal) - 1;
						threshold.set(currentVal, cVal);
						if (labelCounts.get(currentVal) != maxCount) 
						{
							//System.out.println("Changing Continue Running?");
							continueRunning = true;
						}
						currentNode.setLabel(valIndex);
						dominantLabels.clear();
					}
				}
			}
		}
		else
		{
			while(dominantLabels.size() > 0)
			{
				int val = Collections.min(dominantLabels);
				int rand = dominantLabels.indexOf(val);
				if(threshold.get(rand) == partitionSize)
				{
					//System.out.println("Removing Label which exceeds Threashold");
					dominantLabels.remove(rand);
					continue;
				}
				else				
				{
					//System.out.println("In Else");
					int currentVal = currentNode.getLabel();		
					int nVal = threshold.get(rand) + 1;
					threshold.set(rand, nVal);
					int cVal = threshold.get(currentVal) - 1;
					threshold.set(currentVal, cVal);
					currentNode.setLabel(val);
					if (labelCounts.get(currentVal) != maxCount) 
					{
						//System.out.println("Changing Continue Running?");
						continueRunning = true;
					}
					dominantLabels.clear();
				}
			}
		}
		return Boolean.valueOf(continueRunning);
	}
}
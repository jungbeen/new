

public class Snippet {
		  public static void main(String[] args) throws IOException {  
				
			  BPlusTree<Integer, String> wBplusTree = null;
				  
			  String wCommand = "";
			  Scanner wCommandScanner = new Scanner(System.in);
			  while(true)
			  { 
				  System.out.println("1. Create Index");
				  System.out.println("2. Search Data");
				  wCommand = wCommandScanner.nextLine();
			  
				  if(wCommand.equals("1"))
				  {    		  
					  wBplusTree = new BPlusTree<Integer, String>();
		    	  System.out.println("1. Input Source Data File Name");
		    	  System.out.println("2. Default Input Data File Name");
		    	  System.out.println("   [Default Path, Input Data File Name : /home/jblee/PAPER/PAPER2, lineitem10M.txt]");
		    	  
		    		wCommand = wCommandScanner.nextLine();
		    		
	    		  String wInputDataFileName = "";
				    String wHDFSDataFileName = "";
		    	  
		    	  if(wCommand.equals("1"))
		    	  		{		    	  
			    	  System.out.println("Input Input Data File Name : ");
			    		wInputDataFileName = wCommandScanner.nextLine();
			    	  
				    	System.out.println("Input HDFS Data File Name : ");   	
				      wHDFSDataFileName = wCommandScanner.nextLine();
				
				    	wBplusTree.createIndexTree(wBplusTree, wInputDataFileName, wHDFSDataFileName);
		    	  		}
		    	  else
		    	  		{
					    wInputDataFileName = "lineitem10M.txt";
				    	
				    	System.out.println("Input HDFS Data File Name : ");
				      wHDFSDataFileName = wCommandScanner.nextLine();
				    	
				    	wBplusTree.createIndexTree(wBplusTree, wInputDataFileName, wHDFSDataFileName);
		    	  		}
		    	  
				  }
				  else if(wCommand.equals("2"))
				  {
					  if(wBplusTree != null)
					  {
						  int wSearchKey;  
		    	
						  System.out.println("Input Search Key");
						  try
						  {
							  wSearchKey = Integer.parseInt(wCommandScanner.nextLine());
							  wBplusTree.searchData(wSearchKey);
						  }
						  catch(NumberFormatException e)
						  {
							  System.out.println("input int type");
						  }
					  }
					  else
					  {
						  System.out.println("please create index");
					  }
				  }
				  else if(wCommand.equals("q"))
				  {
					  wCommandScanner.close();
					  break;  
				  }
			 
			  }
			  
		  }
		  
	}
	
}
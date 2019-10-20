// Walt Destler
// BPTree.java

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.Console;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;


//  Implements a SortedMap as a B+ tree.
 
public class BPlusTree<K, V> extends AbstractMap<K,V> implements SortedMap<K,V>
{
	
	private static Comparator mDefaultComp = new DefaultComparator();
	
	private Comparator mComp;
	private int mOrder;
	private int mLeafOrder;
	
	private Node mRootNode;
	private int mSize = 0;
	
	private LeafNode mFirstLeaf;
	
	private int mModCount = Integer.MIN_VALUE;
	private Set<Entry<K,V>> mEsInstance = (new SubMap()).entrySet();
	
	private BPlusTree<Integer, String> mIndexBplusTree;
	private String mDataFileName = "datafile.dat";
	
	private Configuration mHadoopConf;
	private FileSystem mHDFS;
	private Path mHadoopDataFileNamePath;  
	
	private int mOffset = 0;
	
//	  Creates a new BPTree of order and leaf order 3 and assumes that all keys implement Comparable.
	BPlusTree()
	{
		this(mDefaultComp, 3, 3);
	}
	
//	  Creates a new BPTree of order and leaf order 3.
//	  @param c Comparator to use to sort objects.
	BPlusTree(Comparator c)
	{
		this(c, 3, 3);
	}
	
//	  Creates a new BPTree and assumes that all keys implement Comparable.
//	  @param order Order of internal guide nodes.
//	  @param leafOrder Order of leaf nodes.
//	  @throws IllegalArgumentException thrown if order < 3 or leafOrder < 1.
	BPlusTree(int order, int leafOrder)
		throws IllegalArgumentException
	{
		this(mDefaultComp, order, leafOrder);
	}
	
//	  Creates a new BPTree.
//	  @param c Comparator to use to sort objects.
//	  @param order Order of internal guide nodes.
//	  @param leafOrder Order of leaf mO.
//	  @throws IllegalArgumentException thrown if order < 3 or leafOrder < 1.	 
	BPlusTree(Comparator c, int order, int leafOrder) throws IllegalArgumentException
	{
		this.mComp = c;
		this.mOrder = order;
		this.mLeafOrder = leafOrder;
		
		mRootNode = mFirstLeaf = new LeafNode();
	}

//	  Returns the first key currently in this BPTree.
	public K firstKey()
	{
		if(mSize == 0)
			throw new NoSuchElementException();
		
		return mFirstLeaf.keys.get(0);
	}
	
//	 Returns the last key currently in this BPTree.
	public K lastKey()
	{
		if(mSize == 0)
			throw new NoSuchElementException();
		
		LeafNode cur = mFirstLeaf;
		while(cur.next != null)
			cur = cur.next;
		
		return cur.keys.get(cur.keys.size() - 1);
	}
	
//	  Returns the comparator associated with this BPTree, or null if it uses its keys' natural ordering.
	public Comparator<K> comparator()
	{
		if(mComp instanceof BPlusTree.DefaultComparator)
			return null;
		else
			return mComp;
	}
	
//	  Returns the number of key-value mappings in this BPTree.
	public int size()
	{
		return mSize;
	}
	
//	  Removes all mappings from this BPTree.
	public void clear()
	{
		mRootNode = mFirstLeaf = new LeafNode();
		mSize = 0;
		mModCount++;
	}
	
//	  Returns true if this BPTree contains a mapping for the specified key.
	public boolean containsKey(Object key)
	{
		Node cur = mRootNode;
		while(cur instanceof BPlusTree.GuideNode)
		{
			GuideNode gn = (GuideNode)cur;
			int index = findGuideIndex(gn, key);
			cur = gn.children.get(index);
		}
		
		LeafNode ln = (LeafNode)cur;
		return findLeafIndex(ln, key) != -1;
	}

//	  Associates the specified value with the specified key in this map.	 
	public V put(K key, V value)
	{
		if(key == null)
			throw new NullPointerException();
		
		// Increment size?
		if(!containsKey(key))
			mSize++;
		
		// Get previous value at the key.
		//V ret = get(key);
		V ret = (V)"1";
		
		// Insert the new key/value into the tree.
		Node newNode = mRootNode.put(key, value);
		
		// Create new root?
		if(newNode != null)
		{
			GuideNode newRoot = new GuideNode();
			newRoot.keys.add(newNode.keys.get(0));
			newRoot.children.add(mRootNode);
			newRoot.children.add(newNode);
			
			mRootNode = newRoot;
		}

		// Increment mod count.
		mModCount++;
		
		// Return the previous value.
		return ret;
	}
	
	public V get(Object key)
	{
		System.out.println(key);
		//int key = ;
		Node cur = mRootNode;
		while(cur instanceof BPlusTree.GuideNode)
		{
			GuideNode gn = (GuideNode)cur;
			int index = findGuideIndex(gn, key);
			cur = gn.children.get(index);
		}
		
		LeafNode ln = (LeafNode)cur;
		int index = findLeafIndex(ln, key);
		if(index == -1)
			return null;
		else
			return (V)ln.values.get(index).toString();
	}
	
//	  Removes the specified key from this map.

//	  Returns a set view of the entries mapped in this BPTree.	 
	public Set<Entry<K, V>> entrySet()
	{
		return mEsInstance;
	}
	
//	  Returns a map representing a sub-range of the keys stored in this BPTree.
	public SortedMap<K, V> subMap(K arg0, K arg1)
	{
		return new SubMap(arg0, arg1);
	}

//	  Returns a map representing a sub-range of the keys stored in this BPTree.	 
	public SortedMap<K, V> headMap(K arg0)
	{
		return subMap(null, arg0);
	}

//	  Returns a map representing a sub-range of the keys stored in this BPTree.	 
	public SortedMap<K, V> tailMap(K arg0)
	{
		return subMap(arg0, null);
	}
	
	
//	  Returns the index to follow in a guide node for the specified key.
	private int findGuideIndex(GuideNode node, Object key)
	{
		for(int i = 1; i < node.keys.size(); i++)
		{
			if(mComp.compare(key, node.keys.get(i)) < 0)
				return i - 1;
		}
		
		return node.keys.size() - 1;
	}
	
//	  Returns the index to follow in a guide node for the specified key.
	private int findLeafIndex(LeafNode node, Object key)
	{
		for(int i = 0; i < node.keys.size(); i++)
		{
			if(mComp.compare(key, node.keys.get(i)) == 0)
				return i;
		}
		
		return -1;
	}
	
//	  Prints this BPTree to the specified StringWriter in XML format.
	public void printXml(StringWriter out)
	{
		Node cur = mRootNode;
		while(cur instanceof BPlusTree.GuideNode)
		{
			cur = ((GuideNode)cur).children.get(0);
		}		
		mRootNode.printXml(out, 4);
	}
	
	public void createIndex(FileWriter out)
	{
		Node cur = mRootNode;
		while(cur instanceof BPlusTree.GuideNode)
		{
			cur = ((GuideNode)cur).children.get(0);
		}
		
		mRootNode.createIndex(out);
	}
	
//	  Base class for tree nodes.
	private abstract class Node
	{
		public ArrayList<K> keys;
		
//		  Maps the specified key to the specified value in this Node.
//		  @return A new right node if this node was split, else null.
		public abstract Node put(K key, V value);
		
		
//		  Prints this Node and all sub-Node to the specified StringWriter in XML format.
		public abstract void printXml(StringWriter out, int indent);
		public abstract void createIndex(FileWriter out);
	}
	
//	  Represents a guide node in the tree.
	private class GuideNode extends Node
	{
		public ArrayList<Node> children;
		
		public GuideNode prev = null;
		public GuideNode next = null;
		
//		  Creates a new GuideNode of the specified order.
		public GuideNode()
		{			
			keys = new ArrayList<K>(mOrder);
			children = new ArrayList<Node>(mOrder);
			
			keys.add(null); // Serves as lower-bound key.
		}
		
//		  Maps the specified key to the specified value in this Node.
//		  @return A new right node if this node was split, else null.
		public Node put(K key, V value)
		{
			GuideNode newGuide = null;
			
			int guideIndex = findGuideIndex(key);
			
			// Recurse to child.
			Node newNode = children.get(guideIndex).put(key, value);
			
			// Did we split?
			if(newNode != null)
			{
				// Insert the new key and node at the found index.
				keys.add(guideIndex + 1, newNode.keys.get(0));
				children.add(guideIndex + 1, newNode);
				
				// Do we need to split?
				if(keys.size() > mOrder)
				{
					newGuide = new GuideNode();
					
					newGuide.keys.clear();
					newGuide.keys.addAll(keys.subList(keys.size() / 2, keys.size()));
					newGuide.children.addAll(children.subList(children.size() / 2, children.size()));
					
					ArrayList<K> newKeys = new ArrayList<K>(mLeafOrder);
					ArrayList<Node> newChildren = new ArrayList<Node>(mLeafOrder);
					
					newKeys.addAll(keys.subList(0, keys.size() / 2));
					newChildren.addAll(children.subList(0, children.size() / 2));
					
					keys = newKeys;
					children = newChildren;
					
					newGuide.next = next;
					newGuide.prev = this;
					if(next != null)
						next.prev = newGuide;
					next = newGuide;
				}
			}
			
			return newGuide;
		}
		
//		  Removes the specified key from this Node.
//		  @return 0 if nothing was removed, 1 if a key was removed but Nodes did not merge,
//		    2 if this Node merged with its left sibling, or 3 if this Node merged with its right sibling.
		 
//		  Returns the guide index of to use when looking for the specified key.
		private int findGuideIndex(Object key)
		{
			return BPlusTree.this.findGuideIndex(this, key);
		}
		
//		  Prints this Node and all sub-Node to the specified StringWriter in XML format.
		public void printXml(StringWriter out, int indent)
		{
			for(int i = 0; i < indent; i++)
				out.write("  ");
			
			// Print first child.
			children.get(0).printXml(out, indent + 1);
			
			// Print each key followed by its greater child.
			for(int i = 1; i < keys.size(); i++)
			{
				K key = keys.get(i);
				Node child = children.get(i);
				
				// Print key.
				for(int j = 0; j < indent + 1; j++)
					out.write("  ");

				child.printXml(out, indent + 1);
			}
			
			for(int i = 0; i < indent; i++)
				out.write("  ");
			out.write("</guide>\n");
			System.out.println(out.toString());
		}
		
		public void createIndex(FileWriter out)
		{
			children.get(0).createIndex(out);
			
			for(int i = 1; i < keys.size(); i++)
			{
				Node child = children.get(i);
				
				child.createIndex(out);
			}
		}

	}
	
	
//	Represents a leaf node in the tree.
	private class LeafNode extends Node
	{
		public ArrayList<ArrayList<V>> values;
		
		private LeafNode prev = null;
		private LeafNode next = null;
		
		
//	Creates a new LeafNode of the specified order.
		 
		public LeafNode()
		{			
			keys = new ArrayList<K>(mLeafOrder);
			// ArrayList로 추가
			values = new ArrayList<ArrayList<V>>(mLeafOrder);
		}
		
		
//		  Maps the specified key to the specified value in this Node.
//		  @return A new right node if this node was split, else null.		 
		public Node put(K key, V value)
		{
			LeafNode newLeaf = null;
			
			// Find insert index.
			int insertIndex = 0;
			while(insertIndex < keys.size())
			{
				if(mComp.compare(key, keys.get(insertIndex)) <= 0)
					break;
				
				insertIndex++;
			}
			
			if(insertIndex < keys.size() && keys.get(insertIndex).equals(key))
			{
				//values.set(insertIndex, value);
					values.get(insertIndex).add(value);
			}
			else
			{
				// Insert the new key and value at the found index.
				keys.add(insertIndex, key);
				
				if(values.size()==0 || checkKey(key))
				{
					
					ArrayList<V> arrayValue = new ArrayList<V>();
					arrayValue.add(value);
					
					values.add(insertIndex, arrayValue);
				}
				else
				{
					values.get(insertIndex).add(value);
				}
				
				// Do we need to split?
				if(keys.size() > mLeafOrder)
				{
					newLeaf = new LeafNode();
					
					newLeaf.keys.addAll(keys.subList(keys.size() / 2, keys.size()));
					newLeaf.values.addAll(values.subList(values.size() / 2, values.size()));
					
					ArrayList<K> newKeys = new ArrayList<K>(mLeafOrder);
					ArrayList<ArrayList<V>> newValues = new ArrayList<ArrayList<V>>(mLeafOrder);
					
					newKeys.addAll(keys.subList(0, keys.size() / 2));
					newValues.addAll(values.subList(0, values.size() / 2));
					
					keys = newKeys;
					values = newValues;
					
					newLeaf.next = next;
					newLeaf.prev = this;
					if(next != null)
						next.prev = newLeaf;
					next = newLeaf;
				}
			}
			
			return newLeaf;
		}
		
		
//		  Removes the specified key from this Node.
//		  @return 0 if nothing was removed, 1 if a key was removed but Nodes did not merge,
//		    2 if this Node merged with its left sibling, or 3 if this Node merged with its right sibling.
//		 	
//		
//		  Returns the guide index of to use when looking for the specified key.	 
		private int findLeafIndex(Object key)
		{
			return BPlusTree.this.findLeafIndex(this, key);
		}
		
//		  Prints this Node and all keys to the specified StringWriter in XML format.	 
		public boolean checkKey(K key)
		{
			boolean result = false;
			for(int i=0; i<keys.size(); i++)
			{
				if(key == keys.get(i))
				{
					return true;
				}
					
			}
			return result;
		}
		
		public void printXml(StringWriter out, int indent)
		{
			for(int i = 0; i < indent; i++)
				out.write("  ");
			out.write("<leaf>\n");
			System.out.println(out.toString());
			
			// Print each entry.
			for(int i = 0; i < keys.size(); i++)
			{
				K key = keys.get(i);
				ArrayList<V> value = values.get(i);
				
				// Print entry.
				for(int j = 0; j < indent + 1; j++)
					out.write("  ");
				out.write("<entry key=\"" + key.toString() + "\" value=\"" + value.toString() + "\"/>\n");
				System.out.println(out.toString());
			}
			
			for(int i = 0; i < indent; i++)
				out.write("  ");
			out.write("</leaf>\n");
			System.out.println(out.toString());
		}		
		
		public void createIndex(FileWriter out)
		{
			for(int i = 0; i < keys.size(); i++)
			{
				K wKey = keys.get(i);
				ArrayList<V> wValueList = values.get(i);
				
				try 
				{
					StringBuilder wValueListBuilder = new StringBuilder();
					
					for(int j=0; j<wValueList.size(); j++)
					{
						wValueListBuilder.append(wValueList.get(j).toString());
						if(!(j==wValueList.size()-1))
						{
							wValueListBuilder.append('\n');
						}
					}
					
					out.write(wValueListBuilder.toString());
					
					mIndexBplusTree.put((Integer)wKey, wKey.toString() + "," + mDataFileName + "," + mOffset + "," + wValueListBuilder.length());
					mOffset += wValueListBuilder.length();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}	
		}

	}
	
//	 Casts objects to Comparable and compares them.
	private static class DefaultComparator<K> implements Comparator<K>
	{
		/**
		 * Casts a to Comparable and compares it to b.
		 */
		public int compare(K a, K b)
		{
			if(a == null)
			{
				if(b == null)
					return 0;
				else
					return -1;
			}
			else
			{
				return ((Comparable)a).compareTo(b);
			}
		}
	}
	
//	  A SortedMap which represents a sub-region of the key-space mapped by a BPTree.
	private class SubMap extends AbstractMap<K,V> implements SortedMap<K,V>
	{
		private K low;
		private K high;
		
		private final EntrySet esInstance = new EntrySet();
		
		
//		  Creates a new SubMap representing the sub-region between low (inclusive) and high (exclusive).
		public SubMap(K low, K high)
		{
			this.low = low;
			this.high = high;
		}
		
		
//		  Creates a new SubMap representing the entire BPTree.
		public SubMap()
		{
			low = null;
			high = null;
		}
		
		
//		  Returns whether the specified key is valid for this SubMap.
		private boolean checkKey(Object key)
		{
			return (low == null || mComp.compare(key, low) >= 0) && (high == null || mComp.compare(key, high) < 0);
		}
		
		
//		  Returns whether this SubMap contains the specified key.
		public boolean containsKey(Object key)
		{
			return checkKey(key) && BPlusTree.this.containsKey(key);
		}
		
		
//		  Returns the value associated with the specified key.
		public V get(Object key)
		{
			if(checkKey(key))
				return BPlusTree.this.get(key);
			else
				return null;
		}
		
		
//		  Associates the specified value with the specified key in this SubMap.
		public V put(K key, V value)
		{
			if(checkKey(key))
				return BPlusTree.this.put(key, value);
			else
				throw new IllegalArgumentException();
		}

		
//		  Returns the Comparator used to compare keys.
		public Comparator comparator()
		{
			return BPlusTree.this.comparator();
		}
		
		
//		  Returns the first key in this SubMap.
		public K firstKey()
		{
			for(K key : this.keySet())
				return key;
			
			throw new NoSuchElementException();
		}
		
		
//		  Returns the last key in this SubMap.
		public K lastKey()
		{
			K key = null;
			for(K k : this.keySet())
				key = k;
			
			if(key == null)
				throw new NoSuchElementException();
			
			return key;
		}
		
		
//		  Returns a Set view of the Entries mapped in this SubMap.
		public Set<Entry<K,V>> entrySet()
		{
			return esInstance;
		}
		
		
//		  Returns a map representing a sub-range of the keys stored in this SubMap.
		public SortedMap<K, V> subMap(K arg0, K arg1)
		{
			// Make sure specified bounds stay within the bounds of THIS SubMap.
			K newLow, newHigh;
			if(arg0 != null && mComp.compare(arg0, low) > 0)
				newLow = arg0;
			else
				newLow = low;
			if(arg1 != null && mComp.compare(arg1, high) < 0)
				newHigh = arg1;
			else
				newHigh = high;
			
			// Return a new SubMap.
			return BPlusTree.this.subMap(newLow, newHigh);
		}

		
//		  Returns a map representing a sub-range of the keys stored in this SubMap.
		public SortedMap<K, V> headMap(K arg0)
		{
			return subMap(firstKey(), arg0);
		}

		
//		  Returns a map representing a sub-range of the keys stored in this SubMap.
		public SortedMap<K, V> tailMap(K arg0)
		{
			return subMap(arg0, lastKey());
		}
		
		
//		  A set of map entries backed by a SubMap.
		private class EntrySet extends AbstractSet<Entry<K,V>>
		{			
			
//			  Removes all entries from this Set.
			public void clear()
			{
				if(low == null && high == null)
					BPlusTree.this.clear();
				else
					super.clear();
			}

			
//			  Returns whether this Set contains the specified entry.
			public boolean contains(Object entry)
			{
				if(entry instanceof Entry)
				{
					Entry<K,V> e = (Entry<K,V>)entry;
					if(SubMap.this.containsKey(e.getKey()))
					{
						V value = SubMap.this.get(e.getKey());
						return value == null ? e.getValue() == null : value.equals(e.getValue());
					}
					else
					{
						return false;
					}
				}
				else
				{
					return false;
				}
			}

			
//			  Returns an iterator which iterates over the elements in this Set.
			public Iterator<Entry<K, V>> iterator()
			{
				return new EntrySetIterator();
			}
//			  Removes the specified entry from this set.
			 
//			  Returns the number of elements in this set.
			public int size()
			{
				if(low == null && high == null)
					return BPlusTree.this.size();
				else
				{
					int count = 0;
					for(Entry<K,V> e : entrySet())
					{
						e = e == null ? null : null; // this line exists only to get rid of the "e is not used" warning.
						count++;
					}
					
					return count;
				}
			}
			
			
//			  Iterates through all of the entries in the set.
			private class EntrySetIterator implements Iterator<Entry<K,V>>
			{
				private int modCount;
				
				private LeafNode curNode;
				private int curIndex = 0;
				private BPTEntry lastEntry = null;
				
				
//				  Creates a new BPTreeIterator.
				public EntrySetIterator()
				{
					modCount = BPlusTree.this.mModCount;
					curNode = BPlusTree.this.mFirstLeaf;
					
					// Keep getting next entry until we reach something >= low.
					if(low != null)
					{
						// Find leaf node that contains the lowest allowable key.
						Node cur = mRootNode;
						while(cur instanceof BPlusTree.GuideNode)
						{
							GuideNode gn = (GuideNode)cur;
							int index = findGuideIndex(gn, low);
							cur = gn.children.get(index);
						}

						curNode = (LeafNode)cur;
						
						// We may need to skip to the next node.
						if(mComp.compare(curNode.keys.get(curNode.keys.size() - 1), low) < 0)
							curNode = curNode.next;
						
						// Find first key >= low.
						if(curNode != null)
						{
							for(curIndex = 0; curIndex < curNode.keys.size() && mComp.compare(curNode.keys.get(curIndex), low) < 0; curIndex++)
								/* empty body */;
						}
					}
				}
				
//				  Returs whether there are any entries left in the iteration.
				public boolean hasNext()
				{
					return curNode != null && curIndex < curNode.keys.size() &&
						(high == null || mComp.compare(curNode.keys.get(curIndex), high) < 0);
				}
				
				
//				  Returns the next entry in the iteration.
				public Entry<K,V> next()
				{
					// Make sure tree has not been modified.
					if(modCount != BPlusTree.this.mModCount)
						throw new ConcurrentModificationException();
					
					// No more entries?
					if(!hasNext())
						throw new NoSuchElementException();
					
					// Get entry.
					lastEntry = new BPTEntry(curNode.keys.get(curIndex), BPlusTree.this);
					
					// Increment index.
					curIndex++;
					if(curIndex >= curNode.keys.size())
					{
						curNode = curNode.next;
						curIndex = 0;
					}
					
					// Return.
					return lastEntry;
				}
			}
		}
	}
	
	
//	  An entry in a BPTree bound to a key and backed by the tree.
	private class BPTEntry implements Entry<K,V>
	{
		private K key;
		private BPlusTree<K,V> tree;
		
//		  Creates a new BPTEntry bound to the specified key and backed by the specified tree.
		public BPTEntry(K key, BPlusTree<K,V> tree)
		{
			this.key = key;
			this.tree = tree;
		}
		
//		  Returns the key to which this entry is bound. 
		public K getKey()
		{
			return key;
		}
		
//		  Returns the value associated with this entry.
		public V getValue()
		{
			return tree.get(key);
		}
		
		
//		  Sets the value associated with this entry. The BPTree will be changed to reflect the new value.
		public V setValue(V value)
		{
			return tree.put(key, value);
		}
	}
	
	  public void createIndexTree(BPlusTree<Integer, String> pBplusTree, String pInputDataFileName, String pHDFSDataFileName) throws IOException
	  {
		  mIndexBplusTree = new BPlusTree<Integer, String>();
		  
		  FileReader wInputDataFileReader = new FileReader("/home/jblee/PAPER/PAPER2/" + pInputDataFileName);
		  BufferedReader wInputDataFileBufferedReader = new BufferedReader(wInputDataFileReader);
		    
		  FileWriter wDataFileWriter = new FileWriter("/home/jblee/PAPER/PAPER2/" + pHDFSDataFileName);
		    
			String wReadLine;
			String wKeyString;
			StringTokenizer wLineStringTokenizer;
			  
			while((wReadLine = wInputDataFileBufferedReader.readLine()) != null)
			{
				wLineStringTokenizer = new StringTokenizer(wReadLine, "|");
				wKeyString = wLineStringTokenizer.nextToken();
				pBplusTree.put(Integer.parseInt(wKeyString), wReadLine);
		    }
			  
			wInputDataFileBufferedReader.close();
			wInputDataFileReader.close();
			  
			long wStartTime = System.currentTimeMillis();	
			  
			pBplusTree.createIndex(wDataFileWriter);
			  
			wDataFileWriter.close();
			
			FileInputStream wDataFileInputStream = new FileInputStream("/home/jblee/PAPER/PAPER2/" + pHDFSDataFileName);
			BufferedInputStream wDataFileBufferedBufferedInputStream = new BufferedInputStream(wDataFileInputStream);

			mHadoopConf = new Configuration();
			mHDFS = FileSystem.get(mHadoopConf);
			mHadoopDataFileNamePath = new Path("PAPER/" + pHDFSDataFileName);  
			FSDataOutputStream wHadoopOutputStream = mHDFS.create(mHadoopDataFileNamePath);
			
			int wReadBytes;
			byte[] wDataFileReadBuffer = new byte[1024];  
			
		  while((wReadBytes = wDataFileBufferedBufferedInputStream.read(wDataFileReadBuffer, 0, 1024)) != -1)
			{
				wHadoopOutputStream.write(wDataFileReadBuffer, 0, wReadBytes);
			}
			  
			wHadoopOutputStream.close();

			wDataFileBufferedBufferedInputStream.close();
			wDataFileInputStream.close();
			  
			long wEndTime   = System.currentTimeMillis();
			  
			long wTotalTime = wEndTime - wStartTime;
			System.out.println("create index time : " + wTotalTime);
	  }
	  
	  public void searchData(int pSearchKey) throws IOException
	  {
		  String wIndexData;
		  String[] wIndexDataArray;
		  int wKeyIndexData;
		  String wDataFileNameIndexData;
		  Long wOffsetIndexData;
		  int wLengthIndexData;
		  
		  long wStartTime = System.currentTimeMillis();	
		  
		  wIndexData = mIndexBplusTree.get(pSearchKey);

		  if(wIndexData != null)
		  {
			  wIndexDataArray = wIndexData.split(",");
		  
			  wKeyIndexData = Integer.parseInt(wIndexDataArray[0].substring(1));
			  wDataFileNameIndexData = wIndexDataArray[1];
			  wOffsetIndexData = Long.parseLong(wIndexDataArray[2]);
			  wLengthIndexData = Integer.parseInt(wIndexDataArray[3].substring(0, wIndexDataArray[3].length()-1));
		  
			  int wReadBytes;
			  int wDividLengthIndexData = wLengthIndexData/1024;
			  int wModLengthIndexData = wLengthIndexData%1024;
			  char[] wHDFSReadBuffer = new char[1024];
			  StringBuilder wHDFSReadResultBuilder = new StringBuilder();
		  
			  InputStreamReader wHadoopInputStreamReader = new InputStreamReader(mHDFS.open(mHadoopDataFileNamePath));
			  BufferedReader wHadoopInputBufferedReader = new BufferedReader(wHadoopInputStreamReader);

			  wHadoopInputBufferedReader.skip(wOffsetIndexData);

			  for(int i=0; i < wDividLengthIndexData+1; i++)
			  {
				  if(wDividLengthIndexData == i)
				  {
					  wReadBytes = wHadoopInputBufferedReader.read(wHDFSReadBuffer, 0, wModLengthIndexData);
				  }
				  else
				  {
					  wReadBytes = wHadoopInputBufferedReader.read(wHDFSReadBuffer, 0, 1024);
				  }
				  wHDFSReadResultBuilder.append(wHDFSReadBuffer, 0, wReadBytes);
			  }
			  
			  wHadoopInputBufferedReader.close();
			  wHadoopInputStreamReader.close();
		    
			  System.out.println(wHDFSReadResultBuilder.toString());
			  
				long wEndTime   = System.currentTimeMillis();
				  
				long wTotalTime = wEndTime - wStartTime;
				System.out.println("search data time : " + wTotalTime);
		  }
		  else
		  {
			  System.out.println("no data");
		  }
		  
	  }
	    
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

// Walt Destler
// BPTree.java

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.Console;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Implements a SortedMap as a B+ tree.
 */

public class IndexBPlusTree<K, V> extends AbstractMap<K,V> implements SortedMap<K,V>
{
	
	private static Comparator defaultComp = new DefaultComparator();
	
	private Comparator comp;
	private int order;
	private int leafOrder;
	
	private Node root;
	private int size = 0;
	
	private LeafNode firstLeaf;
	
	private int modCount = Integer.MIN_VALUE;
	private Set<Entry<K,V>> esInstance = (new SubMap()).entrySet();
	
	static IndexBPlusTree<String, String> iBt = new IndexBPlusTree<String, String>();
	private int offset = 0;
	/**
	 * Creates a new BPTree of order and leaf order 3 and assumes that all keys implement Comparable.
	 */
	IndexBPlusTree()
	{
		this(defaultComp, 3, 3);
	}

	/**
	 * Creates a new BPTree of order and leaf order 3.
	 * @param c Comparator to use to sort objects.
	 */
	IndexBPlusTree(Comparator c)
	{
		this(c, 3, 3);
	}

	/**
	 * Creates a new BPTree and assumes that all keys implement Comparable.
	 * @param order Order of internal guide nodes.
	 * @param leafOrder Order of leaf nodes.
	 * @throws IllegalArgumentException thrown if order < 3 or leafOrder < 1.
	 */
	IndexBPlusTree(int order, int leafOrder)
		throws IllegalArgumentException
	{
		this(defaultComp, order, leafOrder);
	}

	/**
	 * Creates a new BPTree.
	 * @param c Comparator to use to sort objects.
	 * @param order Order of internal guide nodes.
	 * @param leafOrder Order of leaf nodes.
	 * @throws IllegalArgumentException thrown if order < 3 or leafOrder < 1.
	 */
	IndexBPlusTree(Comparator c, int order, int leafOrder)
		throws IllegalArgumentException
	{
		this.comp = c;
		this.order = order;
		this.leafOrder = leafOrder;
		
		root = firstLeaf = new LeafNode();
	}

	/**
	 * Returns the first key currently in this BPTree.
	 */
	public K firstKey()
	{
		if(size == 0)
			throw new NoSuchElementException();
		
		return firstLeaf.keys.get(0);
	}
	
	/**
	 * Returns the last key currently in this BPTree.
	 */
	public K lastKey()
	{
		if(size == 0)
			throw new NoSuchElementException();
		
		LeafNode cur = firstLeaf;
		while(cur.next != null)
			cur = cur.next;
		
		return cur.keys.get(cur.keys.size() - 1);
	}
	
	/**
	 * Returns the comparator associated with this BPTree, or null if it uses its keys' natural ordering.
	 */
	public Comparator<K> comparator()
	{
		if(comp instanceof IndexBPlusTree.DefaultComparator)
			return null;
		else
			return comp;
	}
	
	/**
	 * Returns the number of key-value mappings in this BPTree.
	 */
	public int size()
	{
		return size;
	}
	
	/**
	 * Removes all mappings from this BPTree.
	 */
	public void clear()
	{
		root = firstLeaf = new LeafNode();
		size = 0;
		modCount++;
	}
	
	/**
	 * Returns true if this BPTree contains a mapping for the specified key.
	 */
	public boolean containsKey(Object key)
	{
		Node cur = root;
		while(cur instanceof IndexBPlusTree.GuideNode)
		{
			GuideNode gn = (GuideNode)cur;
			int index = findGuideIndex(gn, key);
			cur = gn.children.get(index);
		}
		
		LeafNode ln = (LeafNode)cur;
		return findLeafIndex(ln, key) != -1;
	}
	
	/**
	 * Returns the value to which this BPTree maps the specified key or null if it contains no mapping for the key.
	 */
//	public V get(Object key)
//	{
//		Node cur = root;
//		while(cur instanceof BPTree.GuideNode)
//		{
//			GuideNode gn = (GuideNode)cur;
//			int index = findGuideIndex(gn, key);
//			cur = gn.children.get(index);
//		}
//		
//		LeafNode ln = (LeafNode)cur;
//		int index = findLeafIndex(ln, key);
//		if(index == -1)
//			return null;
//		else
//			return ln.values.get(index).;
//	}
//	
	/**
	 * Associates the specified value with the specified key in this map.
	 */
	public V put(K key, V value)
	{
		if(key == null)
			throw new NullPointerException();
		
		// Increment size?
		if(!containsKey(key))
			size++;
		
		// Get previous value at the key.
		//V ret = get(key);
		V ret = (V)"1";
		
		// Insert the new key/value into the tree.
		Node newNode = root.put(key, value);
		
		// Create new root?
		if(newNode != null)
		{
			GuideNode newRoot = new GuideNode();
			newRoot.keys.add(newNode.keys.get(0));
			newRoot.children.add(root);
			newRoot.children.add(newNode);
			
			root = newRoot;
		}

		// Increment mod count.
		modCount++;
		
		// Return the previous value.
		return ret;
	}
	
	public V get(Object key)
	{
		Node cur = root;
		while(cur instanceof IndexBPlusTree.GuideNode)
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
	/**
	 * Removes the specified key from this map.
	 */

	/**
	 * Returns a set view of the entries mapped in this BPTree.
	 */
	public Set<Entry<K, V>> entrySet()
	{
		return esInstance;
	}
	
	/**
	 * Returns a map representing a sub-range of the keys stored in this BPTree.
	 */
	public SortedMap<K, V> subMap(K arg0, K arg1)
	{
		return new SubMap(arg0, arg1);
	}

	/**
	 * Returns a map representing a sub-range of the keys stored in this BPTree.
	 */
	public SortedMap<K, V> headMap(K arg0)
	{
		return subMap(null, arg0);
	}

	/**
	 * Returns a map representing a sub-range of the keys stored in this BPTree.
	 */
	public SortedMap<K, V> tailMap(K arg0)
	{
		return subMap(arg0, null);
	}
	
	/**
	 * Returns the index to follow in a guide node for the specified key.
	 */
	private int findGuideIndex(GuideNode node, Object key)
	{
		for(int i = 1; i < node.keys.size(); i++)
		{
			if(comp.compare(key, node.keys.get(i)) < 0)
				return i - 1;
		}
		
		return node.keys.size() - 1;
	}
	
	/**
	 * Returns the index to follow in a guide node for the specified key.
	 */
	private int findLeafIndex(LeafNode node, Object key)
	{
		for(int i = 0; i < node.keys.size(); i++)
		{
			if(comp.compare(key, node.keys.get(i)) == 0)
				return i;
		}
		
		return -1;
	}
	
	/**
	 * Prints this BPTree to the specified StringWriter in XML format.
	 */
	public void printXml(StringWriter out)
	{
		int cardinality = size;
		int height = 1;
		
		// Calc height.
		Node cur = root;
		while(cur instanceof IndexBPlusTree.GuideNode)
		{
			height++;
			cur = ((GuideNode)cur).children.get(0);
		}
		
		// Write.
		//out.write("      <bptree cardinality=\"" + cardinality + "\" height=\"" + height + "\" bpOrder=\"" + order + "\" leafOrder=\"" + leafOrder + "\">\n");
		//System.out.println(out.toString());
		root.printXml(out, 4);
		//out.write("      </bptree>\n");
		//System.out.println(out.toString());
	}
	
	public void printXml(FileWriter out)
	{
		int cardinality = size;
		int height = 1;
		
		// Calc height.
		Node cur = root;
		while(cur instanceof IndexBPlusTree.GuideNode)
		{
			height++;
			cur = ((GuideNode)cur).children.get(0);
		}
		
		// Write.
		//out.write("      <bptree cardinality=\"" + cardinality + "\" height=\"" + height + "\" bpOrder=\"" + order + "\" leafOrder=\"" + leafOrder + "\">\n");
		//System.out.println(out.toString());
		root.printXml(out);
		//out.write("      </bptree>\n");
		//System.out.println(out.toString());
	}
	
	public void printXml(FileWriter out, IndexBPlusTree<K, V> tree)
	{
		int cardinality = size;
		int height = 1;
		
		// Calc height.
		Node cur = root;
		while(cur instanceof IndexBPlusTree.GuideNode)
		{
			height++;
			cur = ((GuideNode)cur).children.get(0);
		}
		
		root.printXml(out,tree);
	}
	/**
	 * Base class for tree nodes.
	 */
	private abstract class Node
	{
		public ArrayList<K> keys;
		
		/**
		 * Maps the specified key to the specified value in this Node.
		 * @return A new right node if this node was split, else null.
		 */
		public abstract Node put(K key, V value);
		
		/**
		 * Prints this Node and all sub-Node to the specified StringWriter in XML format.
		 */
		public abstract void printXml(StringWriter out, int indent);
		public abstract void printXml(FileWriter out);
		public abstract void printXml(FileWriter out, IndexBPlusTree<K, V> tree);
	}
	
	/**
	 * Represents a guide node in the tree.
	 */
	private class GuideNode extends Node
	{
		public ArrayList<Node> children;
		
		public GuideNode prev = null;
		public GuideNode next = null;
		
		/**
		 * Creates a new GuideNode of the specified order.
		 */
		public GuideNode()
		{			
			keys = new ArrayList<K>(order);
			children = new ArrayList<Node>(order);
			
			keys.add(null); // Serves as lower-bound key.
		}
		
		/**
		 * Maps the specified key to the specified value in this Node.
		 * @return A new right node if this node was split, else null.
		 */
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
				if(keys.size() > order)
				{
					newGuide = new GuideNode();
					
					newGuide.keys.clear();
					newGuide.keys.addAll(keys.subList(keys.size() / 2, keys.size()));
					newGuide.children.addAll(children.subList(children.size() / 2, children.size()));
					
					ArrayList<K> newKeys = new ArrayList<K>(leafOrder);
					ArrayList<Node> newChildren = new ArrayList<Node>(leafOrder);
					
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
		
		/**
		 * Removes the specified key from this Node.
		 * @return 0 if nothing was removed, 1 if a key was removed but Nodes did not merge,
		 *   2 if this Node merged with its left sibling, or 3 if this Node merged with its right sibling.
		 */

		
		/**
		 * Returns the guide index of to use when looking for the specified key.
		 */
		private int findGuideIndex(Object key)
		{
			return IndexBPlusTree.this.findGuideIndex(this, key);
		}
		
		/**
		 * Prints this Node and all sub-Node to the specified StringWriter in XML format.
		 */
		public void printXml(StringWriter out, int indent)
		{
			for(int i = 0; i < indent; i++)
				out.write("  ");
	//		out.write("<guide>\n");
		//	System.out.println(out.toString());
			
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
		//		out.write("<key value=\"" + key.toString() + "\"/>\n");
		//		System.out.println(out.toString());
				// Print child.
				child.printXml(out, indent + 1);
			}
			
			for(int i = 0; i < indent; i++)
				out.write("  ");
			out.write("</guide>\n");
			System.out.println(out.toString());
		}
		
		public void printXml(FileWriter out)
		{

			children.get(0).printXml(out);
			
			// Print each key followed by its greater child.
			for(int i = 1; i < keys.size(); i++)
			{
				K key = keys.get(i);
				Node child = children.get(i);
				

				child.printXml(out);
			}

			System.out.println(out.toString());
		}
		public void printXml(FileWriter out, IndexBPlusTree<K, V> tree)
		{

			children.get(0).printXml(out);
			
			// Print each key followed by its greater child.
			for(int i = 1; i < keys.size(); i++)
			{
				K key = keys.get(i);
				Node child = children.get(i);
				

				child.printXml(out,tree);
			}

			System.out.println(out.toString());
		}
	}
	
	/**
	 * Represents a leaf node in the tree.
	 */
	private class LeafNode extends Node
	{
		public ArrayList<ArrayList<V>> values;
		
		private LeafNode prev = null;
		private LeafNode next = null;
		
		/**
		 * Creates a new LeafNode of the specified order.
		 */
		public LeafNode()
		{			
			keys = new ArrayList<K>(leafOrder);
			values = new ArrayList<ArrayList<V>>(leafOrder);
//			for(int i=0; i<leafOrder; i++)
//			{
//				values.add(new ArrayList<V>());
//			}
		}
		
		/**
		 * Maps the specified key to the specified value in this Node.
		 * @return A new right node if this node was split, else null.
		 */
		public Node put(K key, V value)
		{
			LeafNode newLeaf = null;
			
			// Find insert index.
			int insertIndex = 0;
			while(insertIndex < keys.size())
			{
				if(comp.compare(key, keys.get(insertIndex)) <= 0)
					break;
				
				insertIndex++;
			}
			
			if(values.size()== insertIndex)
			{
				
			}
			// If the key already exists, then just replace.
			if(insertIndex < keys.size() && keys.get(insertIndex).equals(key))
			{

	
				//values.set(insertIndex, value);
					values.get(insertIndex).add(value);
			}
			else
			{
				// Insert the new key and value at the found index.
				keys.add(insertIndex, key);
				//svalues.add(insertIndex, value);
				
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
				if(keys.size() > leafOrder)
				{
					newLeaf = new LeafNode();
					
					newLeaf.keys.addAll(keys.subList(keys.size() / 2, keys.size()));
					newLeaf.values.addAll(values.subList(values.size() / 2, values.size()));
					
					ArrayList<K> newKeys = new ArrayList<K>(leafOrder);
					ArrayList<ArrayList<V>> newValues = new ArrayList<ArrayList<V>>(leafOrder);
					
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
		
		/**
		 * Removes the specified key from this Node.
		 * @return 0 if nothing was removed, 1 if a key was removed but Nodes did not merge,
		 *   2 if this Node merged with its left sibling, or 3 if this Node merged with its right sibling.
		 */
		
		/**
		 * Returns the guide index of to use when looking for the specified key.
		 */
		private int findLeafIndex(Object key)
		{
			return IndexBPlusTree.this.findLeafIndex(this, key);
		}
		
		/**
		 * Prints this Node and all keys to the specified StringWriter in XML format.
		 */
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
		
		public void printXml(FileWriter out)
		{

			// Print each entry.
			for(int i = 0; i < keys.size(); i++)
			{
				K key = keys.get(i);
				ArrayList<V> value = values.get(i);
				
				
				//int keyLength = 0;
				// Print entry.
				try 
				{
					StringBuilder sb = new StringBuilder();
					
					for(int j=0; j<value.size(); j++)
					{
						// append�� ��ü
						//keyLength += value.get(j).toString().length();
						sb.append(value.get(j).toString());
						if(!(j==value.size()-1))
							sb.append(",");
					}
					
					//out.write(value.toString());
					//���Ͽ� ����
					//out.write("key : " + key.toString() + " value : " + sb.toString() + "\0");
					out.write(sb.toString());
					
					// �ε��� Ʈ�� ����
					//tbt.put(key.toString(), sb.toString());
					
					System.out.println(sb.length());
				    
					String fileName = "IndexFile.txt";
					
					//offset += keyLenth;
					iBt.put(key.toString(), key.toString() + "," + fileName + "," + offset + "," + sb.length());
					offset += sb.length();
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//���Ͼ���
				//System.out.println(out.toString());
			}	
		}

		public void printXml(FileWriter out, IndexBPlusTree<K, V> tree)
		{

			// Print each entry.
			for(int i = 0; i < keys.size(); i++)
			{
				K key = keys.get(i);
				ArrayList<V> value = values.get(i);
				
				// Print entry.
				try {
					out.write("<entry key=\"" + key.toString() + "\" value=\"" + value.toString() + "\"/>\n");
					tree.put((K)key.toString(), (V) value.toString());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println(out.toString());
			}	
		}
	}

	/**
	 * Casts objects to Comparable and compares them.
	 */
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
	
	/**
	 * A SortedMap which represents a sub-region of the key-space mapped by a BPTree.
	 */
	private class SubMap extends AbstractMap<K,V> implements SortedMap<K,V>
	{
		private K low;
		private K high;
		
		private final EntrySet esInstance = new EntrySet();
		
		/**
		 * Creates a new SubMap representing the sub-region between low (inclusive) and high (exclusive).
		 */
		public SubMap(K low, K high)
		{
			this.low = low;
			this.high = high;
		}
		
		/**
		 * Creates a new SubMap representing the entire BPTree.
		 */
		public SubMap()
		{
			low = null;
			high = null;
		}
		
		/**
		 * Returns whether the specified key is valid for this SubMap.
		 */
		private boolean checkKey(Object key)
		{
			return (low == null || comp.compare(key, low) >= 0) && (high == null || comp.compare(key, high) < 0);
		}
		
		/**
		 * Returns whether this SubMap contains the specified key.
		 */
		public boolean containsKey(Object key)
		{
			return checkKey(key) && IndexBPlusTree.this.containsKey(key);
		}
		
		/**
		 * Returns the value associated with the specified key.
		 */
		public V get(Object key)
		{
			if(checkKey(key))
				return IndexBPlusTree.this.get(key);
			else
				return null;
		}
		
		/**
		 * Associates the specified value with the specified key in this SubMap.
		 */
		public V put(K key, V value)
		{
			if(checkKey(key))
				return IndexBPlusTree.this.put(key, value);
			else
				throw new IllegalArgumentException();
		}

		/**
		 * Returns the Comparator used to compare keys.
		 */
		public Comparator comparator()
		{
			return IndexBPlusTree.this.comparator();
		}
		
		/**
		 * Returns the first key in this SubMap.
		 */
		public K firstKey()
		{
			for(K key : this.keySet())
				return key;
			
			throw new NoSuchElementException();
		}
		
		/**
		 * Returns the last key in this SubMap.
		 */
		public K lastKey()
		{
			K key = null;
			for(K k : this.keySet())
				key = k;
			
			if(key == null)
				throw new NoSuchElementException();
			
			return key;
		}
		
		/**
		 * Returns a Set view of the Entries mapped in this SubMap.
		 */
		public Set<Entry<K,V>> entrySet()
		{
			return esInstance;
		}
		
		/**
		 * Returns a map representing a sub-range of the keys stored in this SubMap.
		 */
		public SortedMap<K, V> subMap(K arg0, K arg1)
		{
			// Make sure specified bounds stay within the bounds of THIS SubMap.
			K newLow, newHigh;
			if(arg0 != null && comp.compare(arg0, low) > 0)
				newLow = arg0;
			else
				newLow = low;
			if(arg1 != null && comp.compare(arg1, high) < 0)
				newHigh = arg1;
			else
				newHigh = high;
			
			// Return a new SubMap.
			return IndexBPlusTree.this.subMap(newLow, newHigh);
		}

		/**
		 * Returns a map representing a sub-range of the keys stored in this SubMap.
		 */
		public SortedMap<K, V> headMap(K arg0)
		{
			return subMap(firstKey(), arg0);
		}

		/**
		 * Returns a map representing a sub-range of the keys stored in this SubMap.
		 */
		public SortedMap<K, V> tailMap(K arg0)
		{
			return subMap(arg0, lastKey());
		}
		
		/**
		 * A set of map entries backed by a SubMap.
		 */
		private class EntrySet extends AbstractSet<Entry<K,V>>
		{			
			/**
			 * Removes all entries from this Set.
			 */
			public void clear()
			{
				if(low == null && high == null)
					IndexBPlusTree.this.clear();
				else
					super.clear();
			}

			/**
			 * Returns whether this Set contains the specified entry.
			 */
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

			/**
			 * Returns an iterator which iterates over the elements in this Set.
			 */
			public Iterator<Entry<K, V>> iterator()
			{
				return new EntrySetIterator();
			}

			/**
			 * Removes the specified entry from this set.
			 */

			/**
			 * Returns the number of elements in this set.
			 */
			public int size()
			{
				if(low == null && high == null)
					return IndexBPlusTree.this.size();
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
			
			/**
			 * Iterates through all of the entries in the set.
			 */
			private class EntrySetIterator implements Iterator<Entry<K,V>>
			{
				private int modCount;
				
				private LeafNode curNode;
				private int curIndex = 0;
				private BPTEntry lastEntry = null;
				
				/**
				 * Creates a new BPTreeIterator.
				 */
				public EntrySetIterator()
				{
					modCount = IndexBPlusTree.this.modCount;
					curNode = IndexBPlusTree.this.firstLeaf;
					
					// Keep getting next entry until we reach something >= low.
					if(low != null)
					{
						// Find leaf node that contains the lowest allowable key.
						Node cur = root;
						while(cur instanceof IndexBPlusTree.GuideNode)
						{
							GuideNode gn = (GuideNode)cur;
							int index = findGuideIndex(gn, low);
							cur = gn.children.get(index);
						}

						curNode = (LeafNode)cur;
						
						// We may need to skip to the next node.
						if(comp.compare(curNode.keys.get(curNode.keys.size() - 1), low) < 0)
							curNode = curNode.next;
						
						// Find first key >= low.
						if(curNode != null)
						{
							for(curIndex = 0; curIndex < curNode.keys.size() && comp.compare(curNode.keys.get(curIndex), low) < 0; curIndex++)
								/* empty body */;
						}
					}
				}
				
				/**
				 * Returs whether there are any entries left in the iteration.
				 */
				public boolean hasNext()
				{
					return curNode != null && curIndex < curNode.keys.size() &&
						(high == null || comp.compare(curNode.keys.get(curIndex), high) < 0);
				}
				
				/**
				 * Returns the next entry in the iteration.
				 */
				public Entry<K,V> next()
				{
					// Make sure tree has not been modified.
					if(modCount != IndexBPlusTree.this.modCount)
						throw new ConcurrentModificationException();
					
					// No more entries?
					if(!hasNext())
						throw new NoSuchElementException();
					
					// Get entry.
					lastEntry = new BPTEntry(curNode.keys.get(curIndex), IndexBPlusTree.this);
					
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
	
	/**
	 * An entry in a BPTree bound to a key and backed by the tree.
	 */
	private class BPTEntry implements Entry<K,V>
	{
		private K key;
		private IndexBPlusTree<K,V> tree;
		
		/**
		 * Creates a new BPTEntry bound to the specified key and backed by the specified tree.
		 */
		public BPTEntry(K key, IndexBPlusTree<K,V> tree)
		{
			this.key = key;
			this.tree = tree;
		}
		
		/**
		 * Returns the key to which this entry is bound.
		 */
		public K getKey()
		{
			return key;
		}
		
		/**
		 * Returns the value associated with this entry.
		 */
		public V getValue()
		{
			return tree.get(key);
		}
		
		/**
		 * Sets the value associated with this entry. The BPTree will be changed to reflect the new value.
		 */
		public V setValue(V value)
		{
			return tree.put(key, value);
		}
	}
	  public static void main(String[] args) throws IOException {
	 
		 // long startTime = System.currentTimeMillis();
		  IndexBPlusTree<Integer, String> bt = new IndexBPlusTree<Integer, String>();
		  ArrayList<String> ar = new ArrayList<String>();
          

		  StringWriter sw = new StringWriter();
		  FileWriter fw = new FileWriter("/home/jblee/PAPER/PAPER2/dataFile.txt");
		  FileWriter fw1 = new FileWriter("/home/jblee/PAPER/PAPER2/IndexFile1.txt");
		  

		  // Test
		  FileReader fr1 = new FileReader("/home/jblee/PAPER/PAPER2/lineitem21.txt");
		  BufferedReader br1 = new BufferedReader(fr1);
		  String s1;
		  while((s1 = br1.readLine()) != null) 
		  { 
			    ar.add(s1);
		  }
		  
		  br1.close();
		  fr1.close();
		  
		  String s2;
		  String s3;
		  for(int i=0; i<ar.size(); i++)
		  {
			  s3 = ar.get(i);
			  StringTokenizer st = new StringTokenizer(s3, "|");
			  s2 = st.nextToken();
			  bt.put(Integer.parseInt(s2), s3); 
		  }
		
		  	  
	//	  bt.printXml(sw);
	//	  bt.printXml(out);
		  
	//	  iBt.printXml(sw);
	//	  tbt.printXml(fw1);
		  
		  fw.close();
		  fw1.close();
		  
		  long startTime = System.currentTimeMillis();
		  System.out.println(bt.get(1));
		  long endTime   = System.currentTimeMillis();
		  
		  char[] buf = new char[1000];
		  char c;
		  FileReader fr = new FileReader("/home/jblee/PAPER/PAPER2/dataFile.txt");
		  
		  fr.read(buf, 0, 715);
		  String ss1 = new String(buf, 0, 715);
		  System.out.println(ss1);
		  
		  fr.read(buf, 0, 129);
		  String ss2 = new String(buf, 0, 129);
		  System.out.println(ss2);
		  fr.close();
		  
		  
		  Configuration conf = new Configuration();
		  FileSystem fs = FileSystem.get(conf);
		  Path filenamePath = new Path("PAPER/dataFile1.txt");
		  FSDataOutputStream out = fs.create(filenamePath);
		  PrintWriter writer  = new PrintWriter(out);
		  
		  for(int i=0; i<ar.size(); i++)
		  {
			  writer.write(ar.get(i));
		  }
		  writer.close();
		  out.close();
		  
		 // long endTime   = System.currentTimeMillis();
		  
		  long totalTime = endTime - startTime;
		  System.out.println("totalTime : " + totalTime);
//		  for(char b:buf)
//		  {
//			  c=(char)b;
//			 // if(b==0)
//				//  break;
//				  //c='-';
//			  // print
//			  System.out.print(c);
//	      }
//		  System.out.println();
//		  String a = new String(buf, 0, 86);
		//  System.out.println(a);
	  }
}

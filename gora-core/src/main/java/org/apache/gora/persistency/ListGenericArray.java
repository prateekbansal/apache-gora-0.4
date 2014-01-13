/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gora.persistency;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;

/**
 * An {@link ArrayList} based implementation of Avro {@link GenericArray}.
 */
public class ListGenericArray<T> implements GenericArray<T>
  , Comparable<ListGenericArray<T>> {

  private static final int LIST_DEFAULT_SIZE = 10;
  
  private List<T> list;
  private Schema schema;

  public ListGenericArray(Schema schema, List<T> list) {
    this.schema = schema;
    this.list = list;
  }

  public ListGenericArray(Schema schema) {
    this(LIST_DEFAULT_SIZE, schema);
  }
  
  public ListGenericArray(int size, Schema schema) {
    this.schema = schema;
    this.list = new ArrayList<T>(size);
  }

  @Override
  public boolean add(T element) {
    list.add(element);
    return true;
  }

  @Override
  public void clear() {
    list.clear();
  }

  @Override
  public T peek() {
    return null;
  }

  @Override
  public void reverse() {
      throw new RuntimeException("Not implemented.");
  }

  @Override
  public List<T> subList(int fromIndex,
                         int toIndex) 
  {
      throw new RuntimeException("Not implemented.");
  }
  

  @Override
  public int size() {
    return list.size();
  }

  @Override
  public Iterator<T> iterator() {
    return list.iterator();
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public int hashCode() {
    return this.list.hashCode();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (!(obj instanceof ListGenericArray)) return false;
    ListGenericArray that = (ListGenericArray)obj;
    if (!schema.equals(that.schema))
      return false;
    return this.compareTo(that) == 0;
  }

  @Override
  public int compareTo(ListGenericArray<T> o) {
    return GenericData.get().compare(this, o, schema);
  }
  
  @Override
  public String toString() {
    return list.toString();
  }

@Override
public void add(int arg0, T arg1) {
	// TODO Auto-generated method stub
	
}

@Override
public boolean addAll(Collection<? extends T> arg0) {
	// TODO Auto-generated method stub
	return false;
}

@Override
public boolean addAll(int arg0, Collection<? extends T> arg1) {
	// TODO Auto-generated method stub
	return false;
}

@Override
public boolean contains(Object arg0) {
	// TODO Auto-generated method stub
	return false;
}

@Override
public boolean containsAll(Collection<?> arg0) {
	// TODO Auto-generated method stub
	return false;
}

@Override
public T get(int arg0) {
	// TODO Auto-generated method stub
	return null;
}

@Override
public int indexOf(Object arg0) {
	// TODO Auto-generated method stub
	return 0;
}

@Override
public boolean isEmpty() {
	// TODO Auto-generated method stub
	return false;
}

@Override
public int lastIndexOf(Object arg0) {
	// TODO Auto-generated method stub
	return 0;
}

@Override
public ListIterator<T> listIterator() {
	// TODO Auto-generated method stub
	return null;
}

@Override
public ListIterator<T> listIterator(int arg0) {
	// TODO Auto-generated method stub
	return null;
}

@Override
public T remove(int arg0) {
	// TODO Auto-generated method stub
	return null;
}

@Override
public boolean remove(Object arg0) {
	// TODO Auto-generated method stub
	return false;
}

@Override
public boolean removeAll(Collection<?> arg0) {
	// TODO Auto-generated method stub
	return false;
}

@Override
public boolean retainAll(Collection<?> arg0) {
	// TODO Auto-generated method stub
	return false;
}

@Override
public T set(int arg0, T arg1) {
	// TODO Auto-generated method stub
	return null;
}

@Override
public Object[] toArray() {
	// TODO Auto-generated method stub
	return null;
}

@Override
public <T> T[] toArray(T[] arg0) {
	// TODO Auto-generated method stub
	return null;
}
}

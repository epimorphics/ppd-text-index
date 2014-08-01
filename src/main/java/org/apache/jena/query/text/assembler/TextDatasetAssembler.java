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

package org.apache.jena.query.text.assembler;

import static org.apache.jena.query.text.assembler.TextVocab.pDataset;
import static org.apache.jena.query.text.assembler.TextVocab.pDocProducer;
import static org.apache.jena.query.text.assembler.TextVocab.pIndex;
import static org.apache.jena.query.text.assembler.TextVocab.textDataset;

import java.lang.reflect.Constructor;

import org.apache.jena.query.text.TextDatasetFactory;
import org.apache.jena.query.text.TextDocProducer;
import org.apache.jena.query.text.TextIndex;

import com.hp.hpl.jena.assembler.Assembler;
import com.hp.hpl.jena.assembler.Mode;
import com.hp.hpl.jena.assembler.assemblers.AssemblerBase;
import com.hp.hpl.jena.assembler.exceptions.AssemblerException;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.util.graph.GraphUtils;

public class TextDatasetAssembler extends AssemblerBase implements Assembler
{
    
    public static Resource getType() { return textDataset ; }
        
    /*
<#text_dataset> rdf:type     text:Dataset ;
    text:dataset <#dataset> ;
    text:index   <#index> ;
    .

    */
    
    @Override
    public Dataset open(Assembler a, Resource root, Mode mode)
    {
        Resource dataset = GraphUtils.getResourceValue(root, pDataset) ;
        Resource index   = GraphUtils.getResourceValue(root, pIndex) ;
        String producer = GraphUtils.getStringValue(root, pDocProducer) ;
        
        Dataset ds = (Dataset)a.open(dataset) ;
        TextIndex textIndex = (TextIndex)a.open(index) ;
        TextDocProducer docProducer;
        if (producer != null) {
            docProducer = getDocProducer(root, producer);
        } else {
        	docProducer = null;
        }
        
        Dataset dst = TextDatasetFactory.create(ds, textIndex, docProducer) ;
        return dst ;
        
    }
    
    @SuppressWarnings("unchecked")
	private TextDocProducer getDocProducer(Resource root, String className) {
    	Class<TextDocProducer> pc;
    	try {
			pc = (Class<TextDocProducer>) Class.forName(className);
		} catch (ClassNotFoundException e) {
			throw new AssemblerException(root, "failed to load class '" + className + "'", e);
		}
    	Constructor<TextDocProducer> constructor;
    	try {
			constructor = pc.getDeclaredConstructor();
		} catch (NoSuchMethodException e) {
			throw new AssemblerException(root, "Class has not default constructor: " + className, e);
		} catch (SecurityException e) {
			throw new AssemblerException(root, "Can't access default constructor for " + className, e);
		}
    	try {
			return constructor.newInstance();
		} catch (Exception e) {
			throw new AssemblerException(root, "Can't create instance of " + className, e);
		}
    }
}


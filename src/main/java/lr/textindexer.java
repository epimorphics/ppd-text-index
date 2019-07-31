/*
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

package lr ;

import org.apache.jena.query.text.DatasetGraphText;
import org.apache.jena.query.text.EntityDefinition;
import org.apache.jena.query.text.TextDatasetFactory;
import org.apache.jena.query.text.TextIndex;
import org.apache.jena.query.text.TextQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jena.cmd.CmdException;
import jena.cmd.ArgDecl;
import arq.cmdline.CmdARQ;

import com.epimorphics.lr.jena.query.text.ProgressMonitor;
import com.epimorphics.lr.jena.query.text.TextIndexer;
import org.apache.jena.query.Dataset;

/**
 * Text indexer application that will read a dataset and index its triples in
 * its text index.
 */
public class textindexer
    extends CmdARQ
{

    private static Logger log = LoggerFactory.getLogger(textindexer.class) ;

    public static final ArgDecl assemblerDescDecl = new ArgDecl(ArgDecl.HasValue, "desc", "dataset") ;

    protected DatasetGraphText dataset      = null ;
    protected TextIndex        textIndex    = null ;
    protected EntityDefinition entityDefinition ;

    static public void main(String... argv) {
        TextQuery.init() ;
        new textindexer(argv).mainRun() ;
    }

    static public void testMain(String... argv) {
        new textindexer(argv).mainMethod() ;
    }

    protected textindexer(String[] argv) {
        super(argv) ;
        super.add(assemblerDescDecl, "--desc=", "Assembler description file") ;
    }

    @Override
    protected void processModulesAndArgs() {
        super.processModulesAndArgs() ;
        // Two forms : with and without arg.
        // Maximises similarity with other tools.
        String file ;

        if ( ! super.contains(assemblerDescDecl) && getNumPositional() == 0 )
            throw new CmdException("No assembler description given") ;

        if ( super.contains(assemblerDescDecl) ) {
            if ( getValues(assemblerDescDecl).size() != 1 )
                throw new CmdException("Multiple assembler descriptions given via --desc") ;
            if ( getPositional().size() != 0 )
                throw new CmdException("Additional assembler descriptions given") ;
            file = getValue(assemblerDescDecl) ;
        } else {
            if ( getNumPositional() != 1 )
                throw new CmdException("Multiple assembler descriptions given as positional arguments") ;
            file = getPositionalArg(0) ;
        }

        if (file == null)
            throw new CmdException("No dataset specified") ;
        // Assumes a single test dataset description in the assembler file.
        Dataset ds = TextDatasetFactory.create(file) ;
        if (ds == null)
            throw new CmdException("No dataset description found") ;
        // get index.
        dataset = (DatasetGraphText)(ds.asDatasetGraph()) ;
        textIndex = dataset.getTextIndex() ;
        if (textIndex == null)
            throw new CmdException("Dataset has no text index") ;
        entityDefinition = textIndex.getDocDef() ;
    }

    @Override
    protected String getSummary() {
        return getCommandName() + " assemblerFile" ;
    }

    @Override
    protected void exec() {
        try { 
        	TextIndexer indexer = new TextIndexer(dataset);
        	ProgressMonitor pm = new ProgressMonitor("properties indexed", log );
        	indexer.index(pm);
        	pm.close(); 
        } finally {
        	dataset.close();
        }
    }
}

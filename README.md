[![Build Status](https://secure.travis-ci.org/rmannibucau/batchee.png)](http://travis-ci.org/rmannibucau/batchee)

## Modules
### impl (aka batchee-impl)
#### Dependency

    <dependency>
      <groupId>org.apache.batchee</groupId>
      <artifactId>batchee-impl</artifactId>
      <version>${batchee.version}</version>
    </dependency>

#### Goal

Implements JBatch (aka JSR 352).

### Shiro
#### Dependency

    <dependency>
      <groupId>org.apache.batchee</groupId>
      <artifactId>batchee-shiro</artifactId>
      <version>${batchee.version}</version>
    </dependency>

#### Goal

A simple integration with Apache Shiro to check permissions when running a batch.

### Extras
#### Dependency

    <dependency>
      <groupId>org.apache.batchee</groupId>
      <artifactId>batchee-extras</artifactId>
      <version>${batchee.version}</version>
    </dependency>

#### Goal

Basic implementations for Readers/Writers/Processors/.... More on it in extensions part.

### BeanIO
#### Dependency

    <dependency>
      <groupId>org.apache.batchee</groupId>
      <artifactId>batchee-beanio</artifactId>
      <version>${batchee.version}</version>
    </dependency>

#### Goal

Basic implementations of a reader and a writer using BeanIO library. Details in extensions part.

# Configuration
## batchee.properties

`batchee.properties` can configure the JBatch container. It will look up in the classloader (advise: put it in container loader).

Here are the available configurable services:

* TransactionManagementService
    * transaction.user-transaction.jndi: when using default TransactionManagementService override the default UserTransaction jndi name
* PersistenceManagerService (service)
    * persistence.database.schema: schema to use for persistence when using JDBC default implementation
    * persistence.database.jndi: jndi name of the datasource to use for persistence when using JDBC default implementation
    * persistence.database.driver: jdbc driver to use for persistence when using JDBC default implementation if no jndi name is provided
    * persistence.database.url: jdbc url to use for persistence when using JDBC default implementation if no jndi name is provided
    * persistence.database.user: jdbc user to use for persistence when using JDBC default implementation if no jndi name is provided
    * persistence.database.password: jdbc password to use for persistence when using JDBC default implementation if no jndi name is provided
* JobStatusManagerService
* BatchThreadPoolService
* BatchKernelService
* JobXMLLoaderService
* BatchArtifactFactory
* SecurityService

To override a service implementation just set the key name (from the previous list) to a qualified name.
For instance to use shiro security service create a batchee.properties with:

    SecurityService = org.apache.batchee.shiro.ShiroSecurityService

# Extensions
## Extras
### `org.apache.batchee.extras.locator.BeanLocator`

Each time an implementation/reference needs to be resolved this API is used. The default one respects the same
rules as the implementation used to resolve ref attributes of the batch xml file (it means you can use qualified names,
CDI names if you are in a CDI container...).

### `org.apache.batchee.extras.chain.ChainProcessor`

Allow to set multiple `javax.batch.api.chunk.ItemProcessor` through a single processor. The n+1 processor processes the
returned value of the n processor.

Sample:

    <step id="step1">
      <chunk>
        <reader ref="..." />
        <processor ref="org.apache.batchee.extras.chain.ChainProcessor">
          <properties>
            <property name="chain" value="ref1,ref2,ref3"/>
          </properties>
        </processor>
        <writer ref="..." />
    </chunk>
  </step>

Note: `org.apache.batchee.extras.chain.ChainBatchlet` does the same for `javax.batch.api.Batchlet`.

### `org.apache.batchee.extras.flat.FlatFileItemReader`

A reader reading line by line a file. By default the line is returned as a `java.lang.String`. To return another object
just override `protected Object preReturn(String line, long lineNumber)` method:

    public class MyFlatReader extends FlatFileItemReader {
        @Override
        protected Object preReturn(String line, long lineNumber) {
            return new Person(line);
        }
    }

Sample:

    <step id="step1">
      <chunk>
        <reader ref="org.apache.batchee.extras.flat.FlatFileItemReader">
          <properties>
            <property name="input" value="#{jobParameters['input']}" />
          </properties>
        </reader>
        <processor ref="..." />
        <writer ref="..." />
      </chunk>
    </step>

Configuration:

* comments: a comma separated list of prefixes marking comment lines
* input: the input file path

### `org.apache.batchee.extras.flat.FlatFileItemWriter`

A writer writing an item by line. By default `toString()` is used on items, to change it
just override `protected String preWrite(Object object)` method:

    public class MyFlatReader extends FlatFileItemReader {
        @Override
        protected String preWrite(final Object object) {
            final Person person = (Person) object;
            return person.getName() + "," + person.getAge();
        }
    }

Sample:

    <step id="step1">
      <chunk>
        <reader ref="..."/>
        <processor ref="..." />
        <writer ref="org.apache.batchee.extras.flat.FlatFileItemWriter">
          <properties>
            <property name="output" value="#{jobParameters['output']}"/>
          </properties>
        </writer>
      </chunk>
    </step>

Configuration:

* encoding: the output file encoding
* output: the output file path
* line.separator: the separator to use, the "line.separator" system property by default

### `org.apache.batchee.extras.jdbc.JdbcReader`

This reader execute a query while the query returns items.

Sample:

    <step id="step1">
      <chunk>
        <reader ref="org.apache.batchee.extras.jdbc.JdbcReader">
          <properties>
            <property name="mapper" value="org.apache.batchee.extras.JdbcReaderTest$SimpleMapper" />
            <property name="query" value="select * from FOO where name like 't%'" />
            <property name="driver" value="org.apache.derby.jdbc.EmbeddedDriver" />
            <property name="url" value="jdbc:derby:memory:jdbcreader;create=true" />
            <property name="user" value="app" />
            <property name="password" value="app" />
          </properties>
        </reader>
        <processor ref="..." />
        <writer ref="..." />
      </chunk>
    </step>

Configuration:

* jndi: jndi name of the datasource to use
* driver: jdbc driver to use if no jndi name was provided
* url: jdbc url to use if no jndi name was provided
* user: jdbc user to use if no jndi name was provided
* password: jdbc password to use if no jndi name was provided
* mapper: the implementation of `org.apache.batchee.extras.jdbc.RecordMapper` to use to convert `java.sql.ResultSet` to objects
* locator: the `org.apache.batchee.extras.locator.BeanLocator` to use to create the mapper
* query: the query used to find items

Here is a sample record mapper deleting items once read (Note: you probably don't want to do so or at least not without a managed datasource):

    public class SimplePersonMapper implements RecordMapper {
        @Override
        public Object map(final ResultSet resultSet) throws SQLException {
            final String name = resultSet.getString("name"); // extract some fields to create an object
            resultSet.deleteRow();
            return new Person(name);
        }
    }

### `org.apache.batchee.extras.jdbc.JdbcWriter`

A writer storing items in a database.

Sample:

    <step id="step1">
      <chunk>
        <reader ref="..."/>
        <processor ref="..." />
        <writer ref="org.apache.batchee.extras.jdbc.JdbcWriter">
          <properties>
            <property name="mapper" value="org.apache.batchee.extras.JdbcWriterTest$SimpleMapper" />
            <property name="sql" value="insert into FOO (name) values(?)" />
            <property name="driver" value="org.apache.derby.jdbc.EmbeddedDriver" />
            <property name="url" value="jdbc:derby:memory:jdbcwriter;create=true" />
            <property name="user" value="app" />
            <property name="password" value="app" />
          </properties>
        </writer>
      </chunk>
    </step>

Configuration:

* jndi: jndi name of the datasource to use
* driver: jdbc driver to use if no jndi name was provided
* url: jdbc url to use if no jndi name was provided
* user: jdbc user to use if no jndi name was provided
* password: jdbc password to use if no jndi name was provided
* mapper: the implementation of `org.apache.batchee.extras.jdbc.ObjectMapper` to use to convert objects to JDBC through `java.sql.PreparedStatement`
* locator: the `org.apache.batchee.extras.locator.BeanLocator` to use to create the mapper
* sql: the sql used to insert records

Here is a sample object mapper:

    public class SimpleMapper implements ObjectMapper {
        @Override
        public void map(final Object item, final PreparedStatement statement) throws SQLException {
            statement.setString(1, item.toString()); // 1 because our insert statement uses values(?)
        }
    }

### `org.apache.batchee.extras.jpa.JpaItemReader`

Reads items from a JPA query.

Sample:

    <step id="step1">
      <chunk>
        <reader ref="org.apache.batchee.extras.jpa.JpaItemReader">
          <properties>
            <property name="entityManagerProvider" value="org.apache.batchee.extras.util.MyProvider" />
            <property name="query" value="select e from Person e" />
          </properties>
        </reader>
        <processor ref="..." />
        <writer ref="..." />
      </chunk>
    </step>

Configuration:

* entityManagerProvider: the `org.apache.batchee.extras.jpa.EntityManagerProvider` (`BeanLocator` semantic)
* parameterProvider: the `org.apache.batchee.extras.jpa.ParameterProvider` (`BeanLocator` semantic)
* locator: the `org.apache.batchee.extras.locator.BeanLocator` used to create `org.apache.batchee.extras.jpa.EntityManagerProvider` and `org.apache.batchee.extras.jpa.ParameterProvider`
* namedQuery: the named query to use
* query: the JPQL query to use if no named query was provided
* pageSize: the paging size
* detachEntities: a boolean to ask the reader to detach entities
* jpaTransaction: should em.getTransaction() be used or not

### `org.apache.batchee.extras.jpa.JpaItemWriter`

Write items through JPA API.

Sample:

    <step id="step1">
      <chunk>
        <reader ref="..." />
        <processor ref="..." />
        <writer ref="org.apache.batchee.extras.jpa.JpaItemWriter">
          <properties>
            <property name="entityManagerProvider" value="org.apache.batchee.extras.util.MyProvider" />
            <property name="jpaTransaction" value="true" />
          </properties>
        </writer>
      </chunk>
    </step>

Configuration:

* entityManagerProvider: the `org.apache.batchee.extras.jpa.EntityManagerProvider` (`BeanLocator` semantic)
* locator: the `org.apache.batchee.extras.locator.BeanLocator` used to create `org.apache.batchee.extras.jpa.EntityManagerProvider`
* useMerge: a boolean to force using merge instead of persist
* jpaTransaction: should em.getTransaction() be used or not

### `org.apache.batchee.extras.noop.NoopItemWriter`

A writer doing nothing (in <chunk/> a writer is mandatory so it can mock one if you don't need one).

Sample:

    <step id="step1">
      <chunk>
        <reader ref="..." />
        <processor ref="..." />
        <writer ref="org.apache.batchee.extras.noop.NoopItemWriter" />
      </chunk>
    </step>

### `org.apache.batchee.extras.typed.Typed[Reader|Processor|Writer]`

Just abstract class allowing to use typed items instead of `Object` from the JBatch API.

### `org.apache.batchee.extras.stax.StaxItemReader`

A reader using StAX API to read a XML file.

Sample:

    <step id="step1">
      <chunk>
        <reader ref="org.apache.batchee.extras.stax.StaxItemReader">
          <properties>
            <property name="input" value="#{jobParameters['input']}"/>
            <property name="marshallingClasses" value="org.apache.batchee.extras.StaxItemReaderTest$Bar"/>
            <property name="tag" value="bar"/>
          </properties>
        </reader>
        <processor ref="..." />
        <writer ref="..." />
      </chunk>
    </step>

Configuration:

* input: the input file
* tag: the tag marking an object to unmarshall
* marshallingClasses: the comma separated list of JAXB classes to use to create the JAXBContext
* marshallingPackage: if no marshallingClasses are provided this package is used to create the JAXBContext

### `org.apache.batchee.extras.stax.StaxItemWriter`

A writer using StAX API to write a XML file.

Sample:

    <step id="step1">
      <chunk>
        <reader ref="..." />
        <processor ref="..." />
        <writer ref="org.apache.batchee.extras.stax.StaxItemWriter">
          <properties>
            <property name="output" value="#{jobParameters['output']}"/>
            <property name="marshallingClasses" value="org.apache.batchee.extras.StaxItemWriterTest$Foo"/>
          </properties>
        </writer>
      </chunk>
    </step>

Configuration:

* output: the output file
* encoding: the output file encoding (default UTF-8)
* version: the output file version (XML, default 1.0)
* rootTag: the output rootTag (default "root")
* marshallingClasses: the comma separated list of JAXB classes to use to create the JAXBContext
* marshallingPackage: if no marshallingClasses are provided this package is used to create the JAXBContext

### `org.apache.batchee.beanio.BeanIOReader`

A reader using BeanIO.

Sample:

    <step id="step1">
      <chunk>
        <reader ref="org.apache.batchee.beanio.BeanIOReader">
          <properties>
            <property name="file" value="#{jobParameters['input']}"/>
            <property name="streamName" value="readerCSV"/>
            <property name="configuration" value="beanio.xml"/>
          </properties>
        </reader>
        <processor ref="..." />
        <writer ref="..." />
      </chunk>
    </step>

Here is the associated beanio.xml:

      <beanio xmlns="http://www.beanio.org/2012/03"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://www.beanio.org/2012/03 http://www.beanio.org/2012/03/mapping.xsd">
      <stream name="readerCSV" format="csv">
        <record name="record1" class="org.apache.batchee.beanio.bean.Record">
          <field name="field1"/>
          <field name="field2"/>
        </record>
      </stream>
    </beanio>

Configuration:

* file: the input file
* streamName: the stream name (from beanio xml file)
* configuration: the beanio xml configuration file
* locale: the locale to use
* errorHandler: the BeanIO error handler to use

### `org.apache.batchee.beanio.BeanIOWriter`

A writer using BeanIO.

Sample:

    <step id="step1">
      <chunk>
        <reader ref="..." />
        <processor ref="..." />
        <writer ref="org.apache.batchee.beanio.BeanIOWriter">
          <properties>
            <property name="file" value="#{jobParameters['output']}"/>
            <property name="streamName" value="writerCSV"/>
            <property name="configuration" value="beanio.xml"/>
          </properties>
        </writer>
      </chunk>
    </step>

Configuration:

* file: the output file
* streamName: the stream name (from beanio xml file)
* configuration: the beanio xml configuration file (from the classloader)
* encoding: the output file encoding
* line.separator: the line separator to use to separate items (default is no line separator)

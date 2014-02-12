package com.guokr.hebo.tap;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import cascading.flow.FlowProcess;
import cascading.flow.planner.Scope;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.SinkMode;
import cascading.tap.SinkTap;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntrySchemeCollector;
import cascading.tuple.TupleException;

public abstract class BaseTemplateTap<Config, Output> extends
    SinkTap<Config, Output> {

  /** Field OPEN_FILES_THRESHOLD_DEFAULT */
  protected static final int OPEN_TAPS_THRESHOLD_DEFAULT = 300;

  private class TemplateCollector extends TupleEntryCollector {
    private final FlowProcess<Config> flowProcess;
    private final Config conf;
    private final Fields parentFields;
    private final Fields pathFields;

    public TemplateCollector(FlowProcess<Config> flowProcess) {
      super(Fields.asDeclaration(getSinkFields()));
      this.flowProcess = flowProcess;
      this.conf = flowProcess.getConfigCopy();
      this.parentFields = parent.getSinkFields();
      this.pathFields = ((TemplateScheme) getScheme()).pathFields;
    }

    private TupleEntryCollector getCollector(String path) {
      TupleEntryCollector collector = collectors.get(path);

      if (collector != null)
        return collector;

      try {

        collector = createTupleEntrySchemeCollector(flowProcess,
            parent, path);

        flowProcess.increment(Counters.Paths_Opened, 1);
      } catch (IOException exception) {
        throw new TapException("unable to open template path: " + path,
            exception);
      }

      if (collectors.size() > openTapsThreshold)
        purgeCollectors();

      collectors.put(path, collector);

      return collector;
    }

    private void purgeCollectors() {
      int numToClose = Math.max(1, (int) (openTapsThreshold * .10));

      Set<String> removeKeys = new HashSet<String>();
      Set<String> keys = collectors.keySet();

      for (String key : keys) {
        if (numToClose-- == 0)
          break;

        removeKeys.add(key);
      }

      for (String removeKey : removeKeys)
        closeCollector(collectors.remove(removeKey));

      flowProcess.increment(Counters.Path_Purges, 1);
    }

    @Override
    public void close() {
      super.close();

      try {
        for (TupleEntryCollector collector : collectors.values())
          closeCollector(collector);
      } finally {
        collectors.clear();
      }
    }

    private void closeCollector(TupleEntryCollector collector) {
      if (collector == null)
        return;

      try {
        collector.close();

        flowProcess.increment(Counters.Paths_Closed, 1);
      } catch (Exception exception) {
        // do nothing
      }
    }

    public void add(TupleEntry tupleEntry) {
      Fields expectedFields = this.tupleEntry.getFields();
      this.tupleEntry = tupleEntry;

      try {
        collect(tupleEntry);
      } catch (IOException exception) {
        throw new TupleException("unable to collect tuple", exception);
      }
    }

    @SuppressWarnings("serial")
    protected void collect(TupleEntry tupleEntry) throws IOException {
      System.out.println("collect invoked");
      if (pathFields != null) {

        String datetime = tupleEntry.getString(0); // 0位置为datetime

        DateTimeFormatter formatter = DateTimeFormat
            .forPattern("yyyy-MM-dd'T'HH:mm:ssZ");
        final DateTime dt = formatter.parseDateTime(datetime);

        Fields fields = tupleEntry.getFields();

        Fields[] timeFields = new Fields[] { new Fields("?year"),
            new Fields("?month"), new Fields("?day"),
            new Fields("?hour"), new Fields("?minute") };
        fields = fields.append(timeFields);

        Tuple tuple = tupleEntry.getTuple();
        tuple = tuple.append(new Tuple(dt.toString(DateTimeFormat.forPattern("yyyy"))),
            new Tuple(dt.toString(DateTimeFormat.forPattern("MM"))),
            new Tuple(dt.toString(DateTimeFormat.forPattern("dd"))),
            new Tuple(dt.toString(DateTimeFormat.forPattern("HH"))),
            new Tuple(dt.toString(DateTimeFormat.forPattern("mm"))));

        TupleEntry heboTupleEntry = new TupleEntry(fields, tuple);

        Tuple pathValues = heboTupleEntry.selectTuple(pathFields);
        String path = pathValues.format(pathTemplate);

        getCollector(path)
            .add(heboTupleEntry.selectTuple(parentFields));

      } else {
        String path = tupleEntry.getTuple().format(pathTemplate);

        getCollector(path).add(tupleEntry);
      }
    }
  }

  /** Field parent */
  protected Tap parent;
  /** Field pathTemplate */
  protected String pathTemplate;
  /** Field keepParentOnDelete */
  protected boolean keepParentOnDelete = false;
  /** Field openTapsThreshold */
  protected int openTapsThreshold = OPEN_TAPS_THRESHOLD_DEFAULT;

  private Granularity input;
  private Granularity output;
  /** Field collectors */
  private final Map<String, TupleEntryCollector> collectors = new LinkedHashMap<String, TupleEntryCollector>(
      1000, .75f, true);

  protected abstract TupleEntrySchemeCollector createTupleEntrySchemeCollector(
      FlowProcess<Config> flowProcess, Tap parent, String path)
      throws IOException;

  protected static Fields getPathFields(Granularity input, Granularity output) {
    Fields pathFields = null;
    int level = output.getValue() - input.getValue();
    switch (level) {
    case 1:
      pathFields = new Fields(output.toField());
      break;
    case 2:
      pathFields = new Fields(output.getPrevious().toField(),
          output.toField());
      break;
    case 3:
      pathFields = new Fields(output.getPrevious().getPrevious()
          .toField(), output.getPrevious().toField(),
          output.toField());
      break;
    }
    return pathFields;
  }

  protected static String getPathTemplate(Granularity input,
      Granularity output) {
    String pathTemplate = null;
    int level = output.getValue() - input.getValue();
    switch (level) {
    case 1:
      pathTemplate = "%s";
      break;
    case 2:
      pathTemplate = "%s/%s";
      break;
    case 3:
      pathTemplate = "%s/%s/%s";
      break;
    }
    return pathTemplate;
  }

  /**
   * Method getParent returns the parent Tap of this TemplateTap object.
   *
   * @return the parent (type Tap) of this TemplateTap object.
   */
  public Tap getParent() {
    return parent;
  }

  /**
   * Method getPathTemplate returns the pathTemplate
   * {@link java.util.Formatter} format String of this TemplateTap object.
   *
   * @return the pathTemplate (type String) of this TemplateTap object.
   */
  public String getPathTemplate() {
    return pathTemplate;
  }

  @Override
  public String getIdentifier() {
    return parent.getIdentifier();
  }

  /**
   * Method getOpenTapsThreshold returns the openTapsThreshold of this
   * TemplateTap object.
   *
   * @return the openTapsThreshold (type int) of this TemplateTap object.
   */
  public int getOpenTapsThreshold() {
    return openTapsThreshold;
  }

  @Override
  public TupleEntryCollector openForWrite(FlowProcess<Config> flowProcess,
      Output output) throws IOException {
    return new TemplateCollector(flowProcess);
  }

  /** @see cascading.tap.Tap#createResource(Object) */
  public boolean createResource(Config conf) throws IOException {
    return parent.createResource(conf);
  }

  /** @see cascading.tap.Tap#deleteResource(Object) */
  public boolean deleteResource(Config conf) throws IOException {
    return keepParentOnDelete || parent.deleteResource(conf);
  }

  @Override
  public boolean commitResource(Config conf) throws IOException {
    return parent.commitResource(conf);
  }

  @Override
  public boolean rollbackResource(Config conf) throws IOException {
    return parent.rollbackResource(conf);
  }

  /** @see cascading.tap.Tap#resourceExists(Object) */
  public boolean resourceExists(Config conf) throws IOException {
    return parent.resourceExists(conf);
  }

  /** @see cascading.tap.Tap#getModifiedTime(Object) */
  @Override
  public long getModifiedTime(Config conf) throws IOException {
    return parent.getModifiedTime(conf);
  }

  @Override
  public Scope outgoingScopeFor(Set<Scope> incomingScopes) {
    return new Scope(getSinkFields());
  }

  @Override
  public boolean equals(Object object) {
    if (this == object)
      return true;
    if (object == null || getClass() != object.getClass())
      return false;
    if (!super.equals(object))
      return false;

    BaseTemplateTap that = (BaseTemplateTap) object;

    if (parent != null ? !parent.equals(that.parent) : that.parent != null)
      return false;
    if (pathTemplate != null ? !pathTemplate.equals(that.pathTemplate)
        : that.pathTemplate != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (parent != null ? parent.hashCode() : 0);
    result = 31 * result
        + (pathTemplate != null ? pathTemplate.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[\"" + parent + "\"]" + "[\""
        + pathTemplate + "\"]";
  }

  public enum Counters {
    Paths_Opened, Paths_Closed, Path_Purges
  }

  protected BaseTemplateTap(Tap parent, Granularity input,
      Granularity output, int openTapsThreshold) {
    super(new TemplateScheme(parent.getScheme(), getPathFields(input,
        output)));
    this.parent = parent;
    this.pathTemplate = getPathTemplate(input, output);
    this.openTapsThreshold = openTapsThreshold;
    this.input = input;
    this.output = output;
  }

  protected BaseTemplateTap(Tap parent, Granularity input,
      Granularity output, SinkMode sinkMode) {
    super(new TemplateScheme(parent.getScheme(), getPathFields(input,
        output)), sinkMode);
    this.parent = parent;
    this.pathTemplate = getPathTemplate(input, output);
  }

  protected BaseTemplateTap(Tap parent, Granularity input,
      Granularity output, SinkMode sinkMode, boolean keepParentOnDelete,
      int openTapsThreshold) {
    super(new TemplateScheme(parent.getScheme(), getPathFields(input,
        output)), sinkMode);
    this.parent = parent;
    this.pathTemplate = getPathTemplate(input, output);
    this.keepParentOnDelete = keepParentOnDelete;
    this.openTapsThreshold = openTapsThreshold;
  }

  public static class TemplateScheme<Config, Output> extends
      Scheme<Config, Void, Output, Void, Void> {
    private final Scheme scheme;
    private final Fields pathFields;

    public TemplateScheme(Scheme scheme) {
      this.scheme = scheme;
      this.pathFields = null;
    }

    public TemplateScheme(Scheme scheme, Fields pathFields) {
      this.scheme = scheme;

      if (pathFields == null || pathFields.isAll())
        this.pathFields = null;
      else if (pathFields.isDefined())
        this.pathFields = pathFields;
      else
        throw new IllegalArgumentException(
            "pathFields must be defined or the ALL substitution, got: "
                + pathFields.printVerbose());
    }

    public Fields getSinkFields() {
      if (pathFields == null || scheme.getSinkFields().isAll())
        return scheme.getSinkFields();

      return Fields.merge(scheme.getSinkFields(), pathFields);
    }

    public void setSinkFields(Fields sinkFields) {
      scheme.setSinkFields(sinkFields);
    }

    public Fields getSourceFields() {
      return scheme.getSourceFields();
    }

    public void setSourceFields(Fields sourceFields) {
      scheme.setSourceFields(sourceFields);
    }

    public int getNumSinkParts() {
      return scheme.getNumSinkParts();
    }

    public void setNumSinkParts(int numSinkParts) {
      scheme.setNumSinkParts(numSinkParts);
    }

    @Override
    public void sourceConfInit(FlowProcess<Config> flowProcess,
        Tap<Config, Void, Output> tap, Config conf) {
      scheme.sourceConfInit(flowProcess, tap, conf);
    }

    @Override
    public void sourcePrepare(FlowProcess<Config> flowProcess,
        SourceCall<Void, Void> sourceCall) throws IOException {
      scheme.sourcePrepare(flowProcess, sourceCall);
    }

    @Override
    public boolean source(FlowProcess<Config> flowProcess,
        SourceCall<Void, Void> sourceCall) throws IOException {
      throw new UnsupportedOperationException("not supported");
    }

    @Override
    public void sourceCleanup(FlowProcess<Config> flowProcess,
        SourceCall<Void, Void> sourceCall) throws IOException {
      scheme.sourceCleanup(flowProcess, sourceCall);
    }

    @Override
    public void sinkConfInit(FlowProcess<Config> flowProcess,
        Tap<Config, Void, Output> tap, Config conf) {
      scheme.sinkConfInit(flowProcess, tap, conf);
    }

    @Override
    public void sinkPrepare(FlowProcess<Config> flowProcess,
        SinkCall<Void, Output> sinkCall) throws IOException {
      scheme.sinkPrepare(flowProcess, sinkCall);
    }

    @Override
    public void sink(FlowProcess<Config> flowProcess,
        SinkCall<Void, Output> sinkCall) throws IOException {
      throw new UnsupportedOperationException("should never be called");
    }

    @Override
    public void sinkCleanup(FlowProcess<Config> flowProcess,
        SinkCall<Void, Output> sinkCall) throws IOException {
      scheme.sinkCleanup(flowProcess, sinkCall);
    }
  }

}


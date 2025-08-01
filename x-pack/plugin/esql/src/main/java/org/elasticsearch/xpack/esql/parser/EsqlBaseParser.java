// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.esql.parser;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class EsqlBaseParser extends ParserConfig {
  static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    LINE_COMMENT=1, MULTILINE_COMMENT=2, WS=3, CHANGE_POINT=4, ENRICH=5, DEV_EXPLAIN=6, 
    COMPLETION=7, DISSECT=8, EVAL=9, GROK=10, LIMIT=11, ROW=12, SAMPLE=13, 
    SORT=14, STATS=15, WHERE=16, DEV_INLINESTATS=17, DEV_RERANK=18, FROM=19, 
    DEV_TIME_SERIES=20, FORK=21, DEV_FUSE=22, JOIN_LOOKUP=23, DEV_JOIN_FULL=24, 
    DEV_JOIN_LEFT=25, DEV_JOIN_RIGHT=26, DEV_LOOKUP=27, MV_EXPAND=28, DROP=29, 
    KEEP=30, DEV_INSIST=31, RENAME=32, SHOW=33, UNKNOWN_CMD=34, CHANGE_POINT_LINE_COMMENT=35, 
    CHANGE_POINT_MULTILINE_COMMENT=36, CHANGE_POINT_WS=37, ENRICH_POLICY_NAME=38, 
    ENRICH_LINE_COMMENT=39, ENRICH_MULTILINE_COMMENT=40, ENRICH_WS=41, ENRICH_FIELD_LINE_COMMENT=42, 
    ENRICH_FIELD_MULTILINE_COMMENT=43, ENRICH_FIELD_WS=44, SETTING=45, SETTING_LINE_COMMENT=46, 
    SETTTING_MULTILINE_COMMENT=47, SETTING_WS=48, EXPLAIN_WS=49, EXPLAIN_LINE_COMMENT=50, 
    EXPLAIN_MULTILINE_COMMENT=51, PIPE=52, QUOTED_STRING=53, INTEGER_LITERAL=54, 
    DECIMAL_LITERAL=55, AND=56, ASC=57, ASSIGN=58, BY=59, CAST_OP=60, COLON=61, 
    COMMA=62, DESC=63, DOT=64, FALSE=65, FIRST=66, IN=67, IS=68, LAST=69, 
    LIKE=70, NOT=71, NULL=72, NULLS=73, ON=74, OR=75, PARAM=76, RLIKE=77, 
    TRUE=78, WITH=79, EQ=80, CIEQ=81, NEQ=82, LT=83, LTE=84, GT=85, GTE=86, 
    PLUS=87, MINUS=88, ASTERISK=89, SLASH=90, PERCENT=91, LEFT_BRACES=92, 
    RIGHT_BRACES=93, DOUBLE_PARAMS=94, NAMED_OR_POSITIONAL_PARAM=95, NAMED_OR_POSITIONAL_DOUBLE_PARAMS=96, 
    OPENING_BRACKET=97, CLOSING_BRACKET=98, LP=99, RP=100, UNQUOTED_IDENTIFIER=101, 
    QUOTED_IDENTIFIER=102, EXPR_LINE_COMMENT=103, EXPR_MULTILINE_COMMENT=104, 
    EXPR_WS=105, METADATA=106, UNQUOTED_SOURCE=107, FROM_LINE_COMMENT=108, 
    FROM_MULTILINE_COMMENT=109, FROM_WS=110, FORK_WS=111, FORK_LINE_COMMENT=112, 
    FORK_MULTILINE_COMMENT=113, JOIN=114, USING=115, JOIN_LINE_COMMENT=116, 
    JOIN_MULTILINE_COMMENT=117, JOIN_WS=118, LOOKUP_LINE_COMMENT=119, LOOKUP_MULTILINE_COMMENT=120, 
    LOOKUP_WS=121, LOOKUP_FIELD_LINE_COMMENT=122, LOOKUP_FIELD_MULTILINE_COMMENT=123, 
    LOOKUP_FIELD_WS=124, MVEXPAND_LINE_COMMENT=125, MVEXPAND_MULTILINE_COMMENT=126, 
    MVEXPAND_WS=127, ID_PATTERN=128, PROJECT_LINE_COMMENT=129, PROJECT_MULTILINE_COMMENT=130, 
    PROJECT_WS=131, AS=132, RENAME_LINE_COMMENT=133, RENAME_MULTILINE_COMMENT=134, 
    RENAME_WS=135, INFO=136, SHOW_LINE_COMMENT=137, SHOW_MULTILINE_COMMENT=138, 
    SHOW_WS=139;
  public static final int
    RULE_singleStatement = 0, RULE_query = 1, RULE_sourceCommand = 2, RULE_processingCommand = 3, 
    RULE_whereCommand = 4, RULE_dataType = 5, RULE_rowCommand = 6, RULE_fields = 7, 
    RULE_field = 8, RULE_rerankFields = 9, RULE_rerankField = 10, RULE_fromCommand = 11, 
    RULE_timeSeriesCommand = 12, RULE_indexPatternAndMetadataFields = 13, 
    RULE_indexPattern = 14, RULE_clusterString = 15, RULE_selectorString = 16, 
    RULE_unquotedIndexString = 17, RULE_indexString = 18, RULE_metadata = 19, 
    RULE_evalCommand = 20, RULE_statsCommand = 21, RULE_aggFields = 22, RULE_aggField = 23, 
    RULE_qualifiedName = 24, RULE_qualifiedNamePattern = 25, RULE_qualifiedNamePatterns = 26, 
    RULE_identifier = 27, RULE_identifierPattern = 28, RULE_parameter = 29, 
    RULE_doubleParameter = 30, RULE_identifierOrParameter = 31, RULE_limitCommand = 32, 
    RULE_sortCommand = 33, RULE_orderExpression = 34, RULE_keepCommand = 35, 
    RULE_dropCommand = 36, RULE_renameCommand = 37, RULE_renameClause = 38, 
    RULE_dissectCommand = 39, RULE_dissectCommandOptions = 40, RULE_dissectCommandOption = 41, 
    RULE_commandNamedParameters = 42, RULE_grokCommand = 43, RULE_mvExpandCommand = 44, 
    RULE_explainCommand = 45, RULE_subqueryExpression = 46, RULE_showCommand = 47, 
    RULE_enrichCommand = 48, RULE_enrichPolicyName = 49, RULE_enrichWithClause = 50, 
    RULE_sampleCommand = 51, RULE_changePointCommand = 52, RULE_forkCommand = 53, 
    RULE_forkSubQueries = 54, RULE_forkSubQuery = 55, RULE_forkSubQueryCommand = 56, 
    RULE_forkSubQueryProcessingCommand = 57, RULE_completionCommand = 58, 
    RULE_lookupCommand = 59, RULE_inlinestatsCommand = 60, RULE_insistCommand = 61, 
    RULE_fuseCommand = 62, RULE_rerankCommand = 63, RULE_booleanExpression = 64, 
    RULE_regexBooleanExpression = 65, RULE_matchBooleanExpression = 66, RULE_valueExpression = 67, 
    RULE_operatorExpression = 68, RULE_primaryExpression = 69, RULE_functionExpression = 70, 
    RULE_functionName = 71, RULE_mapExpression = 72, RULE_entryExpression = 73, 
    RULE_constant = 74, RULE_booleanValue = 75, RULE_numericValue = 76, RULE_decimalValue = 77, 
    RULE_integerValue = 78, RULE_string = 79, RULE_comparisonOperator = 80, 
    RULE_joinCommand = 81, RULE_joinTarget = 82, RULE_joinCondition = 83, 
    RULE_joinPredicate = 84;
  private static String[] makeRuleNames() {
    return new String[] {
      "singleStatement", "query", "sourceCommand", "processingCommand", "whereCommand", 
      "dataType", "rowCommand", "fields", "field", "rerankFields", "rerankField", 
      "fromCommand", "timeSeriesCommand", "indexPatternAndMetadataFields", 
      "indexPattern", "clusterString", "selectorString", "unquotedIndexString", 
      "indexString", "metadata", "evalCommand", "statsCommand", "aggFields", 
      "aggField", "qualifiedName", "qualifiedNamePattern", "qualifiedNamePatterns", 
      "identifier", "identifierPattern", "parameter", "doubleParameter", "identifierOrParameter", 
      "limitCommand", "sortCommand", "orderExpression", "keepCommand", "dropCommand", 
      "renameCommand", "renameClause", "dissectCommand", "dissectCommandOptions", 
      "dissectCommandOption", "commandNamedParameters", "grokCommand", "mvExpandCommand", 
      "explainCommand", "subqueryExpression", "showCommand", "enrichCommand", 
      "enrichPolicyName", "enrichWithClause", "sampleCommand", "changePointCommand", 
      "forkCommand", "forkSubQueries", "forkSubQuery", "forkSubQueryCommand", 
      "forkSubQueryProcessingCommand", "completionCommand", "lookupCommand", 
      "inlinestatsCommand", "insistCommand", "fuseCommand", "rerankCommand", 
      "booleanExpression", "regexBooleanExpression", "matchBooleanExpression", 
      "valueExpression", "operatorExpression", "primaryExpression", "functionExpression", 
      "functionName", "mapExpression", "entryExpression", "constant", "booleanValue", 
      "numericValue", "decimalValue", "integerValue", "string", "comparisonOperator", 
      "joinCommand", "joinTarget", "joinCondition", "joinPredicate"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, null, null, null, "'change_point'", "'enrich'", null, "'completion'", 
      "'dissect'", "'eval'", "'grok'", "'limit'", "'row'", "'sample'", "'sort'", 
      "'stats'", "'where'", null, null, "'from'", null, "'fork'", null, "'lookup'", 
      null, null, null, null, "'mv_expand'", "'drop'", "'keep'", null, "'rename'", 
      "'show'", null, null, null, null, null, null, null, null, null, null, 
      null, null, null, null, null, null, null, null, "'|'", null, null, null, 
      "'and'", "'asc'", "'='", "'by'", "'::'", "':'", "','", "'desc'", "'.'", 
      "'false'", "'first'", "'in'", "'is'", "'last'", "'like'", "'not'", "'null'", 
      "'nulls'", "'on'", "'or'", "'?'", "'rlike'", "'true'", "'with'", "'=='", 
      "'=~'", "'!='", "'<'", "'<='", "'>'", "'>='", "'+'", "'-'", "'*'", "'/'", 
      "'%'", "'{'", "'}'", "'??'", null, null, null, "']'", null, "')'", null, 
      null, null, null, null, "'metadata'", null, null, null, null, null, null, 
      null, "'join'", "'USING'", null, null, null, null, null, null, null, 
      null, null, null, null, null, null, null, null, null, "'as'", null, null, 
      null, "'info'"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, "LINE_COMMENT", "MULTILINE_COMMENT", "WS", "CHANGE_POINT", "ENRICH", 
      "DEV_EXPLAIN", "COMPLETION", "DISSECT", "EVAL", "GROK", "LIMIT", "ROW", 
      "SAMPLE", "SORT", "STATS", "WHERE", "DEV_INLINESTATS", "DEV_RERANK", 
      "FROM", "DEV_TIME_SERIES", "FORK", "DEV_FUSE", "JOIN_LOOKUP", "DEV_JOIN_FULL", 
      "DEV_JOIN_LEFT", "DEV_JOIN_RIGHT", "DEV_LOOKUP", "MV_EXPAND", "DROP", 
      "KEEP", "DEV_INSIST", "RENAME", "SHOW", "UNKNOWN_CMD", "CHANGE_POINT_LINE_COMMENT", 
      "CHANGE_POINT_MULTILINE_COMMENT", "CHANGE_POINT_WS", "ENRICH_POLICY_NAME", 
      "ENRICH_LINE_COMMENT", "ENRICH_MULTILINE_COMMENT", "ENRICH_WS", "ENRICH_FIELD_LINE_COMMENT", 
      "ENRICH_FIELD_MULTILINE_COMMENT", "ENRICH_FIELD_WS", "SETTING", "SETTING_LINE_COMMENT", 
      "SETTTING_MULTILINE_COMMENT", "SETTING_WS", "EXPLAIN_WS", "EXPLAIN_LINE_COMMENT", 
      "EXPLAIN_MULTILINE_COMMENT", "PIPE", "QUOTED_STRING", "INTEGER_LITERAL", 
      "DECIMAL_LITERAL", "AND", "ASC", "ASSIGN", "BY", "CAST_OP", "COLON", 
      "COMMA", "DESC", "DOT", "FALSE", "FIRST", "IN", "IS", "LAST", "LIKE", 
      "NOT", "NULL", "NULLS", "ON", "OR", "PARAM", "RLIKE", "TRUE", "WITH", 
      "EQ", "CIEQ", "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", 
      "SLASH", "PERCENT", "LEFT_BRACES", "RIGHT_BRACES", "DOUBLE_PARAMS", "NAMED_OR_POSITIONAL_PARAM", 
      "NAMED_OR_POSITIONAL_DOUBLE_PARAMS", "OPENING_BRACKET", "CLOSING_BRACKET", 
      "LP", "RP", "UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", "EXPR_LINE_COMMENT", 
      "EXPR_MULTILINE_COMMENT", "EXPR_WS", "METADATA", "UNQUOTED_SOURCE", "FROM_LINE_COMMENT", 
      "FROM_MULTILINE_COMMENT", "FROM_WS", "FORK_WS", "FORK_LINE_COMMENT", 
      "FORK_MULTILINE_COMMENT", "JOIN", "USING", "JOIN_LINE_COMMENT", "JOIN_MULTILINE_COMMENT", 
      "JOIN_WS", "LOOKUP_LINE_COMMENT", "LOOKUP_MULTILINE_COMMENT", "LOOKUP_WS", 
      "LOOKUP_FIELD_LINE_COMMENT", "LOOKUP_FIELD_MULTILINE_COMMENT", "LOOKUP_FIELD_WS", 
      "MVEXPAND_LINE_COMMENT", "MVEXPAND_MULTILINE_COMMENT", "MVEXPAND_WS", 
      "ID_PATTERN", "PROJECT_LINE_COMMENT", "PROJECT_MULTILINE_COMMENT", "PROJECT_WS", 
      "AS", "RENAME_LINE_COMMENT", "RENAME_MULTILINE_COMMENT", "RENAME_WS", 
      "INFO", "SHOW_LINE_COMMENT", "SHOW_MULTILINE_COMMENT", "SHOW_WS"
    };
  }
  private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
  public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

  /**
   * @deprecated Use {@link #VOCABULARY} instead.
   */
  @Deprecated
  public static final String[] tokenNames;
  static {
    tokenNames = new String[_SYMBOLIC_NAMES.length];
    for (int i = 0; i < tokenNames.length; i++) {
      tokenNames[i] = VOCABULARY.getLiteralName(i);
      if (tokenNames[i] == null) {
        tokenNames[i] = VOCABULARY.getSymbolicName(i);
      }

      if (tokenNames[i] == null) {
        tokenNames[i] = "<INVALID>";
      }
    }
  }

  @Override
  @Deprecated
  public String[] getTokenNames() {
    return tokenNames;
  }

  @Override

  public Vocabulary getVocabulary() {
    return VOCABULARY;
  }

  @Override
  public String getGrammarFileName() { return "EsqlBaseParser.g4"; }

  @Override
  public String[] getRuleNames() { return ruleNames; }

  @Override
  public String getSerializedATN() { return _serializedATN; }

  @Override
  public ATN getATN() { return _ATN; }

  @SuppressWarnings("this-escape")
  public EsqlBaseParser(TokenStream input) {
    super(input);
    _interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
  }

  @SuppressWarnings("CheckReturnValue")
  public static class SingleStatementContext extends ParserRuleContext {
    public QueryContext query() {
      return getRuleContext(QueryContext.class,0);
    }
    public TerminalNode EOF() { return getToken(EsqlBaseParser.EOF, 0); }
    @SuppressWarnings("this-escape")
    public SingleStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_singleStatement; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSingleStatement(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSingleStatement(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSingleStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SingleStatementContext singleStatement() throws RecognitionException {
    SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
    enterRule(_localctx, 0, RULE_singleStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(170);
      query(0);
      setState(171);
      match(EOF);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class QueryContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public QueryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_query; }
   
    @SuppressWarnings("this-escape")
    public QueryContext() { }
    public void copyFrom(QueryContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class CompositeQueryContext extends QueryContext {
    public QueryContext query() {
      return getRuleContext(QueryContext.class,0);
    }
    public TerminalNode PIPE() { return getToken(EsqlBaseParser.PIPE, 0); }
    public ProcessingCommandContext processingCommand() {
      return getRuleContext(ProcessingCommandContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public CompositeQueryContext(QueryContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterCompositeQuery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitCompositeQuery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitCompositeQuery(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class SingleCommandQueryContext extends QueryContext {
    public SourceCommandContext sourceCommand() {
      return getRuleContext(SourceCommandContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public SingleCommandQueryContext(QueryContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSingleCommandQuery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSingleCommandQuery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSingleCommandQuery(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QueryContext query() throws RecognitionException {
    return query(0);
  }

  private QueryContext query(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    QueryContext _localctx = new QueryContext(_ctx, _parentState);
    QueryContext _prevctx = _localctx;
    int _startState = 2;
    enterRecursionRule(_localctx, 2, RULE_query, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      {
      _localctx = new SingleCommandQueryContext(_localctx);
      _ctx = _localctx;
      _prevctx = _localctx;

      setState(174);
      sourceCommand();
      }
      _ctx.stop = _input.LT(-1);
      setState(181);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          {
          _localctx = new CompositeQueryContext(new QueryContext(_parentctx, _parentState));
          pushNewRecursionContext(_localctx, _startState, RULE_query);
          setState(176);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(177);
          match(PIPE);
          setState(178);
          processingCommand();
          }
          } 
        }
        setState(183);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class SourceCommandContext extends ParserRuleContext {
    public FromCommandContext fromCommand() {
      return getRuleContext(FromCommandContext.class,0);
    }
    public RowCommandContext rowCommand() {
      return getRuleContext(RowCommandContext.class,0);
    }
    public ShowCommandContext showCommand() {
      return getRuleContext(ShowCommandContext.class,0);
    }
    public TimeSeriesCommandContext timeSeriesCommand() {
      return getRuleContext(TimeSeriesCommandContext.class,0);
    }
    public ExplainCommandContext explainCommand() {
      return getRuleContext(ExplainCommandContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public SourceCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_sourceCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSourceCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSourceCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSourceCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SourceCommandContext sourceCommand() throws RecognitionException {
    SourceCommandContext _localctx = new SourceCommandContext(_ctx, getState());
    enterRule(_localctx, 4, RULE_sourceCommand);
    try {
      setState(191);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(184);
        fromCommand();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(185);
        rowCommand();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(186);
        showCommand();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(187);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(188);
        timeSeriesCommand();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(189);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(190);
        explainCommand();
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ProcessingCommandContext extends ParserRuleContext {
    public EvalCommandContext evalCommand() {
      return getRuleContext(EvalCommandContext.class,0);
    }
    public WhereCommandContext whereCommand() {
      return getRuleContext(WhereCommandContext.class,0);
    }
    public KeepCommandContext keepCommand() {
      return getRuleContext(KeepCommandContext.class,0);
    }
    public LimitCommandContext limitCommand() {
      return getRuleContext(LimitCommandContext.class,0);
    }
    public StatsCommandContext statsCommand() {
      return getRuleContext(StatsCommandContext.class,0);
    }
    public SortCommandContext sortCommand() {
      return getRuleContext(SortCommandContext.class,0);
    }
    public DropCommandContext dropCommand() {
      return getRuleContext(DropCommandContext.class,0);
    }
    public RenameCommandContext renameCommand() {
      return getRuleContext(RenameCommandContext.class,0);
    }
    public DissectCommandContext dissectCommand() {
      return getRuleContext(DissectCommandContext.class,0);
    }
    public GrokCommandContext grokCommand() {
      return getRuleContext(GrokCommandContext.class,0);
    }
    public EnrichCommandContext enrichCommand() {
      return getRuleContext(EnrichCommandContext.class,0);
    }
    public MvExpandCommandContext mvExpandCommand() {
      return getRuleContext(MvExpandCommandContext.class,0);
    }
    public JoinCommandContext joinCommand() {
      return getRuleContext(JoinCommandContext.class,0);
    }
    public ChangePointCommandContext changePointCommand() {
      return getRuleContext(ChangePointCommandContext.class,0);
    }
    public CompletionCommandContext completionCommand() {
      return getRuleContext(CompletionCommandContext.class,0);
    }
    public SampleCommandContext sampleCommand() {
      return getRuleContext(SampleCommandContext.class,0);
    }
    public ForkCommandContext forkCommand() {
      return getRuleContext(ForkCommandContext.class,0);
    }
    public InlinestatsCommandContext inlinestatsCommand() {
      return getRuleContext(InlinestatsCommandContext.class,0);
    }
    public LookupCommandContext lookupCommand() {
      return getRuleContext(LookupCommandContext.class,0);
    }
    public InsistCommandContext insistCommand() {
      return getRuleContext(InsistCommandContext.class,0);
    }
    public RerankCommandContext rerankCommand() {
      return getRuleContext(RerankCommandContext.class,0);
    }
    public FuseCommandContext fuseCommand() {
      return getRuleContext(FuseCommandContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ProcessingCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_processingCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterProcessingCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitProcessingCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitProcessingCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ProcessingCommandContext processingCommand() throws RecognitionException {
    ProcessingCommandContext _localctx = new ProcessingCommandContext(_ctx, getState());
    enterRule(_localctx, 6, RULE_processingCommand);
    try {
      setState(220);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(193);
        evalCommand();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(194);
        whereCommand();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(195);
        keepCommand();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(196);
        limitCommand();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(197);
        statsCommand();
        }
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        {
        setState(198);
        sortCommand();
        }
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        {
        setState(199);
        dropCommand();
        }
        break;
      case 8:
        enterOuterAlt(_localctx, 8);
        {
        setState(200);
        renameCommand();
        }
        break;
      case 9:
        enterOuterAlt(_localctx, 9);
        {
        setState(201);
        dissectCommand();
        }
        break;
      case 10:
        enterOuterAlt(_localctx, 10);
        {
        setState(202);
        grokCommand();
        }
        break;
      case 11:
        enterOuterAlt(_localctx, 11);
        {
        setState(203);
        enrichCommand();
        }
        break;
      case 12:
        enterOuterAlt(_localctx, 12);
        {
        setState(204);
        mvExpandCommand();
        }
        break;
      case 13:
        enterOuterAlt(_localctx, 13);
        {
        setState(205);
        joinCommand();
        }
        break;
      case 14:
        enterOuterAlt(_localctx, 14);
        {
        setState(206);
        changePointCommand();
        }
        break;
      case 15:
        enterOuterAlt(_localctx, 15);
        {
        setState(207);
        completionCommand();
        }
        break;
      case 16:
        enterOuterAlt(_localctx, 16);
        {
        setState(208);
        sampleCommand();
        }
        break;
      case 17:
        enterOuterAlt(_localctx, 17);
        {
        setState(209);
        forkCommand();
        }
        break;
      case 18:
        enterOuterAlt(_localctx, 18);
        {
        setState(210);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(211);
        inlinestatsCommand();
        }
        break;
      case 19:
        enterOuterAlt(_localctx, 19);
        {
        setState(212);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(213);
        lookupCommand();
        }
        break;
      case 20:
        enterOuterAlt(_localctx, 20);
        {
        setState(214);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(215);
        insistCommand();
        }
        break;
      case 21:
        enterOuterAlt(_localctx, 21);
        {
        setState(216);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(217);
        rerankCommand();
        }
        break;
      case 22:
        enterOuterAlt(_localctx, 22);
        {
        setState(218);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(219);
        fuseCommand();
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class WhereCommandContext extends ParserRuleContext {
    public TerminalNode WHERE() { return getToken(EsqlBaseParser.WHERE, 0); }
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public WhereCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_whereCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterWhereCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitWhereCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitWhereCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final WhereCommandContext whereCommand() throws RecognitionException {
    WhereCommandContext _localctx = new WhereCommandContext(_ctx, getState());
    enterRule(_localctx, 8, RULE_whereCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(222);
      match(WHERE);
      setState(223);
      booleanExpression(0);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class DataTypeContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public DataTypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_dataType; }
   
    @SuppressWarnings("this-escape")
    public DataTypeContext() { }
    public void copyFrom(DataTypeContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ToDataTypeContext extends DataTypeContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ToDataTypeContext(DataTypeContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterToDataType(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitToDataType(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitToDataType(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DataTypeContext dataType() throws RecognitionException {
    DataTypeContext _localctx = new DataTypeContext(_ctx, getState());
    enterRule(_localctx, 10, RULE_dataType);
    try {
      _localctx = new ToDataTypeContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(225);
      identifier();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class RowCommandContext extends ParserRuleContext {
    public TerminalNode ROW() { return getToken(EsqlBaseParser.ROW, 0); }
    public FieldsContext fields() {
      return getRuleContext(FieldsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public RowCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_rowCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRowCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRowCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRowCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RowCommandContext rowCommand() throws RecognitionException {
    RowCommandContext _localctx = new RowCommandContext(_ctx, getState());
    enterRule(_localctx, 12, RULE_rowCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(227);
      match(ROW);
      setState(228);
      fields();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class FieldsContext extends ParserRuleContext {
    public List<FieldContext> field() {
      return getRuleContexts(FieldContext.class);
    }
    public FieldContext field(int i) {
      return getRuleContext(FieldContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public FieldsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_fields; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFields(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFields(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFields(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FieldsContext fields() throws RecognitionException {
    FieldsContext _localctx = new FieldsContext(_ctx, getState());
    enterRule(_localctx, 14, RULE_fields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(230);
      field();
      setState(235);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,3,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(231);
          match(COMMA);
          setState(232);
          field();
          }
          } 
        }
        setState(237);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,3,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class FieldContext extends ParserRuleContext {
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    @SuppressWarnings("this-escape")
    public FieldContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_field; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterField(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitField(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitField(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FieldContext field() throws RecognitionException {
    FieldContext _localctx = new FieldContext(_ctx, getState());
    enterRule(_localctx, 16, RULE_field);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(241);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
      case 1:
        {
        setState(238);
        qualifiedName();
        setState(239);
        match(ASSIGN);
        }
        break;
      }
      setState(243);
      booleanExpression(0);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class RerankFieldsContext extends ParserRuleContext {
    public List<RerankFieldContext> rerankField() {
      return getRuleContexts(RerankFieldContext.class);
    }
    public RerankFieldContext rerankField(int i) {
      return getRuleContext(RerankFieldContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public RerankFieldsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_rerankFields; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRerankFields(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRerankFields(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRerankFields(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RerankFieldsContext rerankFields() throws RecognitionException {
    RerankFieldsContext _localctx = new RerankFieldsContext(_ctx, getState());
    enterRule(_localctx, 18, RULE_rerankFields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(245);
      rerankField();
      setState(250);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,5,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(246);
          match(COMMA);
          setState(247);
          rerankField();
          }
          } 
        }
        setState(252);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,5,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class RerankFieldContext extends ParserRuleContext {
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public RerankFieldContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_rerankField; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRerankField(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRerankField(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRerankField(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RerankFieldContext rerankField() throws RecognitionException {
    RerankFieldContext _localctx = new RerankFieldContext(_ctx, getState());
    enterRule(_localctx, 20, RULE_rerankField);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(253);
      qualifiedName();
      setState(256);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
      case 1:
        {
        setState(254);
        match(ASSIGN);
        setState(255);
        booleanExpression(0);
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class FromCommandContext extends ParserRuleContext {
    public TerminalNode FROM() { return getToken(EsqlBaseParser.FROM, 0); }
    public IndexPatternAndMetadataFieldsContext indexPatternAndMetadataFields() {
      return getRuleContext(IndexPatternAndMetadataFieldsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public FromCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_fromCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFromCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFromCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFromCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FromCommandContext fromCommand() throws RecognitionException {
    FromCommandContext _localctx = new FromCommandContext(_ctx, getState());
    enterRule(_localctx, 22, RULE_fromCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(258);
      match(FROM);
      setState(259);
      indexPatternAndMetadataFields();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class TimeSeriesCommandContext extends ParserRuleContext {
    public TerminalNode DEV_TIME_SERIES() { return getToken(EsqlBaseParser.DEV_TIME_SERIES, 0); }
    public IndexPatternAndMetadataFieldsContext indexPatternAndMetadataFields() {
      return getRuleContext(IndexPatternAndMetadataFieldsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public TimeSeriesCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_timeSeriesCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterTimeSeriesCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitTimeSeriesCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitTimeSeriesCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TimeSeriesCommandContext timeSeriesCommand() throws RecognitionException {
    TimeSeriesCommandContext _localctx = new TimeSeriesCommandContext(_ctx, getState());
    enterRule(_localctx, 24, RULE_timeSeriesCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(261);
      match(DEV_TIME_SERIES);
      setState(262);
      indexPatternAndMetadataFields();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class IndexPatternAndMetadataFieldsContext extends ParserRuleContext {
    public List<IndexPatternContext> indexPattern() {
      return getRuleContexts(IndexPatternContext.class);
    }
    public IndexPatternContext indexPattern(int i) {
      return getRuleContext(IndexPatternContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    public MetadataContext metadata() {
      return getRuleContext(MetadataContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public IndexPatternAndMetadataFieldsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_indexPatternAndMetadataFields; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIndexPatternAndMetadataFields(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIndexPatternAndMetadataFields(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIndexPatternAndMetadataFields(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IndexPatternAndMetadataFieldsContext indexPatternAndMetadataFields() throws RecognitionException {
    IndexPatternAndMetadataFieldsContext _localctx = new IndexPatternAndMetadataFieldsContext(_ctx, getState());
    enterRule(_localctx, 26, RULE_indexPatternAndMetadataFields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(264);
      indexPattern();
      setState(269);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,7,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(265);
          match(COMMA);
          setState(266);
          indexPattern();
          }
          } 
        }
        setState(271);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,7,_ctx);
      }
      setState(273);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
      case 1:
        {
        setState(272);
        metadata();
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class IndexPatternContext extends ParserRuleContext {
    public ClusterStringContext clusterString() {
      return getRuleContext(ClusterStringContext.class,0);
    }
    public TerminalNode COLON() { return getToken(EsqlBaseParser.COLON, 0); }
    public UnquotedIndexStringContext unquotedIndexString() {
      return getRuleContext(UnquotedIndexStringContext.class,0);
    }
    public TerminalNode CAST_OP() { return getToken(EsqlBaseParser.CAST_OP, 0); }
    public SelectorStringContext selectorString() {
      return getRuleContext(SelectorStringContext.class,0);
    }
    public IndexStringContext indexString() {
      return getRuleContext(IndexStringContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public IndexPatternContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_indexPattern; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIndexPattern(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIndexPattern(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIndexPattern(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IndexPatternContext indexPattern() throws RecognitionException {
    IndexPatternContext _localctx = new IndexPatternContext(_ctx, getState());
    enterRule(_localctx, 28, RULE_indexPattern);
    try {
      setState(284);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(275);
        clusterString();
        setState(276);
        match(COLON);
        setState(277);
        unquotedIndexString();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(279);
        unquotedIndexString();
        setState(280);
        match(CAST_OP);
        setState(281);
        selectorString();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(283);
        indexString();
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ClusterStringContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_SOURCE() { return getToken(EsqlBaseParser.UNQUOTED_SOURCE, 0); }
    @SuppressWarnings("this-escape")
    public ClusterStringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_clusterString; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterClusterString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitClusterString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitClusterString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ClusterStringContext clusterString() throws RecognitionException {
    ClusterStringContext _localctx = new ClusterStringContext(_ctx, getState());
    enterRule(_localctx, 30, RULE_clusterString);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(286);
      match(UNQUOTED_SOURCE);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class SelectorStringContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_SOURCE() { return getToken(EsqlBaseParser.UNQUOTED_SOURCE, 0); }
    @SuppressWarnings("this-escape")
    public SelectorStringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_selectorString; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSelectorString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSelectorString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSelectorString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SelectorStringContext selectorString() throws RecognitionException {
    SelectorStringContext _localctx = new SelectorStringContext(_ctx, getState());
    enterRule(_localctx, 32, RULE_selectorString);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(288);
      match(UNQUOTED_SOURCE);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class UnquotedIndexStringContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_SOURCE() { return getToken(EsqlBaseParser.UNQUOTED_SOURCE, 0); }
    @SuppressWarnings("this-escape")
    public UnquotedIndexStringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_unquotedIndexString; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterUnquotedIndexString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitUnquotedIndexString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitUnquotedIndexString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final UnquotedIndexStringContext unquotedIndexString() throws RecognitionException {
    UnquotedIndexStringContext _localctx = new UnquotedIndexStringContext(_ctx, getState());
    enterRule(_localctx, 34, RULE_unquotedIndexString);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(290);
      match(UNQUOTED_SOURCE);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class IndexStringContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_SOURCE() { return getToken(EsqlBaseParser.UNQUOTED_SOURCE, 0); }
    public TerminalNode QUOTED_STRING() { return getToken(EsqlBaseParser.QUOTED_STRING, 0); }
    @SuppressWarnings("this-escape")
    public IndexStringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_indexString; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIndexString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIndexString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIndexString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IndexStringContext indexString() throws RecognitionException {
    IndexStringContext _localctx = new IndexStringContext(_ctx, getState());
    enterRule(_localctx, 36, RULE_indexString);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(292);
      _la = _input.LA(1);
      if ( !(_la==QUOTED_STRING || _la==UNQUOTED_SOURCE) ) {
      _errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class MetadataContext extends ParserRuleContext {
    public TerminalNode METADATA() { return getToken(EsqlBaseParser.METADATA, 0); }
    public List<TerminalNode> UNQUOTED_SOURCE() { return getTokens(EsqlBaseParser.UNQUOTED_SOURCE); }
    public TerminalNode UNQUOTED_SOURCE(int i) {
      return getToken(EsqlBaseParser.UNQUOTED_SOURCE, i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public MetadataContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_metadata; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMetadata(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMetadata(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMetadata(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MetadataContext metadata() throws RecognitionException {
    MetadataContext _localctx = new MetadataContext(_ctx, getState());
    enterRule(_localctx, 38, RULE_metadata);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(294);
      match(METADATA);
      setState(295);
      match(UNQUOTED_SOURCE);
      setState(300);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,10,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(296);
          match(COMMA);
          setState(297);
          match(UNQUOTED_SOURCE);
          }
          } 
        }
        setState(302);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,10,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class EvalCommandContext extends ParserRuleContext {
    public TerminalNode EVAL() { return getToken(EsqlBaseParser.EVAL, 0); }
    public FieldsContext fields() {
      return getRuleContext(FieldsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public EvalCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_evalCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterEvalCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitEvalCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitEvalCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EvalCommandContext evalCommand() throws RecognitionException {
    EvalCommandContext _localctx = new EvalCommandContext(_ctx, getState());
    enterRule(_localctx, 40, RULE_evalCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(303);
      match(EVAL);
      setState(304);
      fields();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class StatsCommandContext extends ParserRuleContext {
    public AggFieldsContext stats;
    public FieldsContext grouping;
    public TerminalNode STATS() { return getToken(EsqlBaseParser.STATS, 0); }
    public TerminalNode BY() { return getToken(EsqlBaseParser.BY, 0); }
    public AggFieldsContext aggFields() {
      return getRuleContext(AggFieldsContext.class,0);
    }
    public FieldsContext fields() {
      return getRuleContext(FieldsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public StatsCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_statsCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterStatsCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitStatsCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitStatsCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StatsCommandContext statsCommand() throws RecognitionException {
    StatsCommandContext _localctx = new StatsCommandContext(_ctx, getState());
    enterRule(_localctx, 42, RULE_statsCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(306);
      match(STATS);
      setState(308);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
      case 1:
        {
        setState(307);
        ((StatsCommandContext)_localctx).stats = aggFields();
        }
        break;
      }
      setState(312);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
      case 1:
        {
        setState(310);
        match(BY);
        setState(311);
        ((StatsCommandContext)_localctx).grouping = fields();
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class AggFieldsContext extends ParserRuleContext {
    public List<AggFieldContext> aggField() {
      return getRuleContexts(AggFieldContext.class);
    }
    public AggFieldContext aggField(int i) {
      return getRuleContext(AggFieldContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public AggFieldsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_aggFields; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterAggFields(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitAggFields(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitAggFields(this);
      else return visitor.visitChildren(this);
    }
  }

  public final AggFieldsContext aggFields() throws RecognitionException {
    AggFieldsContext _localctx = new AggFieldsContext(_ctx, getState());
    enterRule(_localctx, 44, RULE_aggFields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(314);
      aggField();
      setState(319);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,13,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(315);
          match(COMMA);
          setState(316);
          aggField();
          }
          } 
        }
        setState(321);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,13,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class AggFieldContext extends ParserRuleContext {
    public FieldContext field() {
      return getRuleContext(FieldContext.class,0);
    }
    public TerminalNode WHERE() { return getToken(EsqlBaseParser.WHERE, 0); }
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public AggFieldContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_aggField; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterAggField(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitAggField(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitAggField(this);
      else return visitor.visitChildren(this);
    }
  }

  public final AggFieldContext aggField() throws RecognitionException {
    AggFieldContext _localctx = new AggFieldContext(_ctx, getState());
    enterRule(_localctx, 46, RULE_aggField);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(322);
      field();
      setState(325);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
      case 1:
        {
        setState(323);
        match(WHERE);
        setState(324);
        booleanExpression(0);
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class QualifiedNameContext extends ParserRuleContext {
    public List<IdentifierOrParameterContext> identifierOrParameter() {
      return getRuleContexts(IdentifierOrParameterContext.class);
    }
    public IdentifierOrParameterContext identifierOrParameter(int i) {
      return getRuleContext(IdentifierOrParameterContext.class,i);
    }
    public List<TerminalNode> DOT() { return getTokens(EsqlBaseParser.DOT); }
    public TerminalNode DOT(int i) {
      return getToken(EsqlBaseParser.DOT, i);
    }
    @SuppressWarnings("this-escape")
    public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_qualifiedName; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterQualifiedName(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitQualifiedName(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitQualifiedName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QualifiedNameContext qualifiedName() throws RecognitionException {
    QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
    enterRule(_localctx, 48, RULE_qualifiedName);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(327);
      identifierOrParameter();
      setState(332);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,15,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(328);
          match(DOT);
          setState(329);
          identifierOrParameter();
          }
          } 
        }
        setState(334);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,15,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class QualifiedNamePatternContext extends ParserRuleContext {
    public List<IdentifierPatternContext> identifierPattern() {
      return getRuleContexts(IdentifierPatternContext.class);
    }
    public IdentifierPatternContext identifierPattern(int i) {
      return getRuleContext(IdentifierPatternContext.class,i);
    }
    public List<TerminalNode> DOT() { return getTokens(EsqlBaseParser.DOT); }
    public TerminalNode DOT(int i) {
      return getToken(EsqlBaseParser.DOT, i);
    }
    @SuppressWarnings("this-escape")
    public QualifiedNamePatternContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_qualifiedNamePattern; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterQualifiedNamePattern(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitQualifiedNamePattern(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitQualifiedNamePattern(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QualifiedNamePatternContext qualifiedNamePattern() throws RecognitionException {
    QualifiedNamePatternContext _localctx = new QualifiedNamePatternContext(_ctx, getState());
    enterRule(_localctx, 50, RULE_qualifiedNamePattern);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(335);
      identifierPattern();
      setState(340);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,16,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(336);
          match(DOT);
          setState(337);
          identifierPattern();
          }
          } 
        }
        setState(342);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,16,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class QualifiedNamePatternsContext extends ParserRuleContext {
    public List<QualifiedNamePatternContext> qualifiedNamePattern() {
      return getRuleContexts(QualifiedNamePatternContext.class);
    }
    public QualifiedNamePatternContext qualifiedNamePattern(int i) {
      return getRuleContext(QualifiedNamePatternContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public QualifiedNamePatternsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_qualifiedNamePatterns; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterQualifiedNamePatterns(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitQualifiedNamePatterns(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitQualifiedNamePatterns(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QualifiedNamePatternsContext qualifiedNamePatterns() throws RecognitionException {
    QualifiedNamePatternsContext _localctx = new QualifiedNamePatternsContext(_ctx, getState());
    enterRule(_localctx, 52, RULE_qualifiedNamePatterns);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(343);
      qualifiedNamePattern();
      setState(348);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,17,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(344);
          match(COMMA);
          setState(345);
          qualifiedNamePattern();
          }
          } 
        }
        setState(350);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,17,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class IdentifierContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.UNQUOTED_IDENTIFIER, 0); }
    public TerminalNode QUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.QUOTED_IDENTIFIER, 0); }
    @SuppressWarnings("this-escape")
    public IdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_identifier; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIdentifier(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIdentifier(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IdentifierContext identifier() throws RecognitionException {
    IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
    enterRule(_localctx, 54, RULE_identifier);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(351);
      _la = _input.LA(1);
      if ( !(_la==UNQUOTED_IDENTIFIER || _la==QUOTED_IDENTIFIER) ) {
      _errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class IdentifierPatternContext extends ParserRuleContext {
    public TerminalNode ID_PATTERN() { return getToken(EsqlBaseParser.ID_PATTERN, 0); }
    public ParameterContext parameter() {
      return getRuleContext(ParameterContext.class,0);
    }
    public DoubleParameterContext doubleParameter() {
      return getRuleContext(DoubleParameterContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public IdentifierPatternContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_identifierPattern; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIdentifierPattern(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIdentifierPattern(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIdentifierPattern(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IdentifierPatternContext identifierPattern() throws RecognitionException {
    IdentifierPatternContext _localctx = new IdentifierPatternContext(_ctx, getState());
    enterRule(_localctx, 56, RULE_identifierPattern);
    try {
      setState(356);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case ID_PATTERN:
        enterOuterAlt(_localctx, 1);
        {
        setState(353);
        match(ID_PATTERN);
        }
        break;
      case PARAM:
      case NAMED_OR_POSITIONAL_PARAM:
        enterOuterAlt(_localctx, 2);
        {
        setState(354);
        parameter();
        }
        break;
      case DOUBLE_PARAMS:
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
        enterOuterAlt(_localctx, 3);
        {
        setState(355);
        doubleParameter();
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ParameterContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public ParameterContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_parameter; }
   
    @SuppressWarnings("this-escape")
    public ParameterContext() { }
    public void copyFrom(ParameterContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class InputNamedOrPositionalParamContext extends ParameterContext {
    public TerminalNode NAMED_OR_POSITIONAL_PARAM() { return getToken(EsqlBaseParser.NAMED_OR_POSITIONAL_PARAM, 0); }
    @SuppressWarnings("this-escape")
    public InputNamedOrPositionalParamContext(ParameterContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInputNamedOrPositionalParam(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInputNamedOrPositionalParam(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInputNamedOrPositionalParam(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class InputParamContext extends ParameterContext {
    public TerminalNode PARAM() { return getToken(EsqlBaseParser.PARAM, 0); }
    @SuppressWarnings("this-escape")
    public InputParamContext(ParameterContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInputParam(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInputParam(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInputParam(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ParameterContext parameter() throws RecognitionException {
    ParameterContext _localctx = new ParameterContext(_ctx, getState());
    enterRule(_localctx, 58, RULE_parameter);
    try {
      setState(360);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case PARAM:
        _localctx = new InputParamContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(358);
        match(PARAM);
        }
        break;
      case NAMED_OR_POSITIONAL_PARAM:
        _localctx = new InputNamedOrPositionalParamContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(359);
        match(NAMED_OR_POSITIONAL_PARAM);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class DoubleParameterContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public DoubleParameterContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_doubleParameter; }
   
    @SuppressWarnings("this-escape")
    public DoubleParameterContext() { }
    public void copyFrom(DoubleParameterContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class InputDoubleParamsContext extends DoubleParameterContext {
    public TerminalNode DOUBLE_PARAMS() { return getToken(EsqlBaseParser.DOUBLE_PARAMS, 0); }
    @SuppressWarnings("this-escape")
    public InputDoubleParamsContext(DoubleParameterContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInputDoubleParams(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInputDoubleParams(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInputDoubleParams(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class InputNamedOrPositionalDoubleParamsContext extends DoubleParameterContext {
    public TerminalNode NAMED_OR_POSITIONAL_DOUBLE_PARAMS() { return getToken(EsqlBaseParser.NAMED_OR_POSITIONAL_DOUBLE_PARAMS, 0); }
    @SuppressWarnings("this-escape")
    public InputNamedOrPositionalDoubleParamsContext(DoubleParameterContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInputNamedOrPositionalDoubleParams(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInputNamedOrPositionalDoubleParams(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInputNamedOrPositionalDoubleParams(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DoubleParameterContext doubleParameter() throws RecognitionException {
    DoubleParameterContext _localctx = new DoubleParameterContext(_ctx, getState());
    enterRule(_localctx, 60, RULE_doubleParameter);
    try {
      setState(364);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case DOUBLE_PARAMS:
        _localctx = new InputDoubleParamsContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(362);
        match(DOUBLE_PARAMS);
        }
        break;
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
        _localctx = new InputNamedOrPositionalDoubleParamsContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(363);
        match(NAMED_OR_POSITIONAL_DOUBLE_PARAMS);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class IdentifierOrParameterContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public ParameterContext parameter() {
      return getRuleContext(ParameterContext.class,0);
    }
    public DoubleParameterContext doubleParameter() {
      return getRuleContext(DoubleParameterContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public IdentifierOrParameterContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_identifierOrParameter; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIdentifierOrParameter(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIdentifierOrParameter(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIdentifierOrParameter(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IdentifierOrParameterContext identifierOrParameter() throws RecognitionException {
    IdentifierOrParameterContext _localctx = new IdentifierOrParameterContext(_ctx, getState());
    enterRule(_localctx, 62, RULE_identifierOrParameter);
    try {
      setState(369);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case UNQUOTED_IDENTIFIER:
      case QUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(366);
        identifier();
        }
        break;
      case PARAM:
      case NAMED_OR_POSITIONAL_PARAM:
        enterOuterAlt(_localctx, 2);
        {
        setState(367);
        parameter();
        }
        break;
      case DOUBLE_PARAMS:
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
        enterOuterAlt(_localctx, 3);
        {
        setState(368);
        doubleParameter();
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class LimitCommandContext extends ParserRuleContext {
    public TerminalNode LIMIT() { return getToken(EsqlBaseParser.LIMIT, 0); }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public LimitCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_limitCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLimitCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLimitCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLimitCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LimitCommandContext limitCommand() throws RecognitionException {
    LimitCommandContext _localctx = new LimitCommandContext(_ctx, getState());
    enterRule(_localctx, 64, RULE_limitCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(371);
      match(LIMIT);
      setState(372);
      constant();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class SortCommandContext extends ParserRuleContext {
    public TerminalNode SORT() { return getToken(EsqlBaseParser.SORT, 0); }
    public List<OrderExpressionContext> orderExpression() {
      return getRuleContexts(OrderExpressionContext.class);
    }
    public OrderExpressionContext orderExpression(int i) {
      return getRuleContext(OrderExpressionContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public SortCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_sortCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSortCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSortCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSortCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SortCommandContext sortCommand() throws RecognitionException {
    SortCommandContext _localctx = new SortCommandContext(_ctx, getState());
    enterRule(_localctx, 66, RULE_sortCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(374);
      match(SORT);
      setState(375);
      orderExpression();
      setState(380);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,22,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(376);
          match(COMMA);
          setState(377);
          orderExpression();
          }
          } 
        }
        setState(382);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,22,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class OrderExpressionContext extends ParserRuleContext {
    public Token ordering;
    public Token nullOrdering;
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    public TerminalNode NULLS() { return getToken(EsqlBaseParser.NULLS, 0); }
    public TerminalNode ASC() { return getToken(EsqlBaseParser.ASC, 0); }
    public TerminalNode DESC() { return getToken(EsqlBaseParser.DESC, 0); }
    public TerminalNode FIRST() { return getToken(EsqlBaseParser.FIRST, 0); }
    public TerminalNode LAST() { return getToken(EsqlBaseParser.LAST, 0); }
    @SuppressWarnings("this-escape")
    public OrderExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_orderExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterOrderExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitOrderExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitOrderExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final OrderExpressionContext orderExpression() throws RecognitionException {
    OrderExpressionContext _localctx = new OrderExpressionContext(_ctx, getState());
    enterRule(_localctx, 68, RULE_orderExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(383);
      booleanExpression(0);
      setState(385);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
      case 1:
        {
        setState(384);
        ((OrderExpressionContext)_localctx).ordering = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==ASC || _la==DESC) ) {
          ((OrderExpressionContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        }
        break;
      }
      setState(389);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
      case 1:
        {
        setState(387);
        match(NULLS);
        setState(388);
        ((OrderExpressionContext)_localctx).nullOrdering = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==FIRST || _la==LAST) ) {
          ((OrderExpressionContext)_localctx).nullOrdering = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class KeepCommandContext extends ParserRuleContext {
    public TerminalNode KEEP() { return getToken(EsqlBaseParser.KEEP, 0); }
    public QualifiedNamePatternsContext qualifiedNamePatterns() {
      return getRuleContext(QualifiedNamePatternsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public KeepCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_keepCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterKeepCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitKeepCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitKeepCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final KeepCommandContext keepCommand() throws RecognitionException {
    KeepCommandContext _localctx = new KeepCommandContext(_ctx, getState());
    enterRule(_localctx, 70, RULE_keepCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(391);
      match(KEEP);
      setState(392);
      qualifiedNamePatterns();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class DropCommandContext extends ParserRuleContext {
    public TerminalNode DROP() { return getToken(EsqlBaseParser.DROP, 0); }
    public QualifiedNamePatternsContext qualifiedNamePatterns() {
      return getRuleContext(QualifiedNamePatternsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public DropCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_dropCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDropCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDropCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDropCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DropCommandContext dropCommand() throws RecognitionException {
    DropCommandContext _localctx = new DropCommandContext(_ctx, getState());
    enterRule(_localctx, 72, RULE_dropCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(394);
      match(DROP);
      setState(395);
      qualifiedNamePatterns();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class RenameCommandContext extends ParserRuleContext {
    public TerminalNode RENAME() { return getToken(EsqlBaseParser.RENAME, 0); }
    public List<RenameClauseContext> renameClause() {
      return getRuleContexts(RenameClauseContext.class);
    }
    public RenameClauseContext renameClause(int i) {
      return getRuleContext(RenameClauseContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public RenameCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_renameCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRenameCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRenameCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRenameCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RenameCommandContext renameCommand() throws RecognitionException {
    RenameCommandContext _localctx = new RenameCommandContext(_ctx, getState());
    enterRule(_localctx, 74, RULE_renameCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(397);
      match(RENAME);
      setState(398);
      renameClause();
      setState(403);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,25,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(399);
          match(COMMA);
          setState(400);
          renameClause();
          }
          } 
        }
        setState(405);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,25,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class RenameClauseContext extends ParserRuleContext {
    public QualifiedNamePatternContext oldName;
    public QualifiedNamePatternContext newName;
    public TerminalNode AS() { return getToken(EsqlBaseParser.AS, 0); }
    public List<QualifiedNamePatternContext> qualifiedNamePattern() {
      return getRuleContexts(QualifiedNamePatternContext.class);
    }
    public QualifiedNamePatternContext qualifiedNamePattern(int i) {
      return getRuleContext(QualifiedNamePatternContext.class,i);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    @SuppressWarnings("this-escape")
    public RenameClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_renameClause; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRenameClause(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRenameClause(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRenameClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RenameClauseContext renameClause() throws RecognitionException {
    RenameClauseContext _localctx = new RenameClauseContext(_ctx, getState());
    enterRule(_localctx, 76, RULE_renameClause);
    try {
      setState(414);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(406);
        ((RenameClauseContext)_localctx).oldName = qualifiedNamePattern();
        setState(407);
        match(AS);
        setState(408);
        ((RenameClauseContext)_localctx).newName = qualifiedNamePattern();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(410);
        ((RenameClauseContext)_localctx).newName = qualifiedNamePattern();
        setState(411);
        match(ASSIGN);
        setState(412);
        ((RenameClauseContext)_localctx).oldName = qualifiedNamePattern();
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class DissectCommandContext extends ParserRuleContext {
    public TerminalNode DISSECT() { return getToken(EsqlBaseParser.DISSECT, 0); }
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public DissectCommandOptionsContext dissectCommandOptions() {
      return getRuleContext(DissectCommandOptionsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public DissectCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_dissectCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDissectCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDissectCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDissectCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DissectCommandContext dissectCommand() throws RecognitionException {
    DissectCommandContext _localctx = new DissectCommandContext(_ctx, getState());
    enterRule(_localctx, 78, RULE_dissectCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(416);
      match(DISSECT);
      setState(417);
      primaryExpression(0);
      setState(418);
      string();
      setState(420);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,27,_ctx) ) {
      case 1:
        {
        setState(419);
        dissectCommandOptions();
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class DissectCommandOptionsContext extends ParserRuleContext {
    public List<DissectCommandOptionContext> dissectCommandOption() {
      return getRuleContexts(DissectCommandOptionContext.class);
    }
    public DissectCommandOptionContext dissectCommandOption(int i) {
      return getRuleContext(DissectCommandOptionContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public DissectCommandOptionsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_dissectCommandOptions; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDissectCommandOptions(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDissectCommandOptions(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDissectCommandOptions(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DissectCommandOptionsContext dissectCommandOptions() throws RecognitionException {
    DissectCommandOptionsContext _localctx = new DissectCommandOptionsContext(_ctx, getState());
    enterRule(_localctx, 80, RULE_dissectCommandOptions);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(422);
      dissectCommandOption();
      setState(427);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,28,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(423);
          match(COMMA);
          setState(424);
          dissectCommandOption();
          }
          } 
        }
        setState(429);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,28,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class DissectCommandOptionContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public DissectCommandOptionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_dissectCommandOption; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDissectCommandOption(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDissectCommandOption(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDissectCommandOption(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DissectCommandOptionContext dissectCommandOption() throws RecognitionException {
    DissectCommandOptionContext _localctx = new DissectCommandOptionContext(_ctx, getState());
    enterRule(_localctx, 82, RULE_dissectCommandOption);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(430);
      identifier();
      setState(431);
      match(ASSIGN);
      setState(432);
      constant();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class CommandNamedParametersContext extends ParserRuleContext {
    public TerminalNode WITH() { return getToken(EsqlBaseParser.WITH, 0); }
    public MapExpressionContext mapExpression() {
      return getRuleContext(MapExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public CommandNamedParametersContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_commandNamedParameters; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterCommandNamedParameters(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitCommandNamedParameters(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitCommandNamedParameters(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CommandNamedParametersContext commandNamedParameters() throws RecognitionException {
    CommandNamedParametersContext _localctx = new CommandNamedParametersContext(_ctx, getState());
    enterRule(_localctx, 84, RULE_commandNamedParameters);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(436);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,29,_ctx) ) {
      case 1:
        {
        setState(434);
        match(WITH);
        setState(435);
        mapExpression();
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class GrokCommandContext extends ParserRuleContext {
    public TerminalNode GROK() { return getToken(EsqlBaseParser.GROK, 0); }
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public GrokCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_grokCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterGrokCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitGrokCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitGrokCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final GrokCommandContext grokCommand() throws RecognitionException {
    GrokCommandContext _localctx = new GrokCommandContext(_ctx, getState());
    enterRule(_localctx, 86, RULE_grokCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(438);
      match(GROK);
      setState(439);
      primaryExpression(0);
      setState(440);
      string();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class MvExpandCommandContext extends ParserRuleContext {
    public TerminalNode MV_EXPAND() { return getToken(EsqlBaseParser.MV_EXPAND, 0); }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public MvExpandCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_mvExpandCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMvExpandCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMvExpandCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMvExpandCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MvExpandCommandContext mvExpandCommand() throws RecognitionException {
    MvExpandCommandContext _localctx = new MvExpandCommandContext(_ctx, getState());
    enterRule(_localctx, 88, RULE_mvExpandCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(442);
      match(MV_EXPAND);
      setState(443);
      qualifiedName();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ExplainCommandContext extends ParserRuleContext {
    public TerminalNode DEV_EXPLAIN() { return getToken(EsqlBaseParser.DEV_EXPLAIN, 0); }
    public SubqueryExpressionContext subqueryExpression() {
      return getRuleContext(SubqueryExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ExplainCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_explainCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterExplainCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitExplainCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitExplainCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExplainCommandContext explainCommand() throws RecognitionException {
    ExplainCommandContext _localctx = new ExplainCommandContext(_ctx, getState());
    enterRule(_localctx, 90, RULE_explainCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(445);
      match(DEV_EXPLAIN);
      setState(446);
      subqueryExpression();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class SubqueryExpressionContext extends ParserRuleContext {
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public QueryContext query() {
      return getRuleContext(QueryContext.class,0);
    }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    @SuppressWarnings("this-escape")
    public SubqueryExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_subqueryExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSubqueryExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSubqueryExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSubqueryExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SubqueryExpressionContext subqueryExpression() throws RecognitionException {
    SubqueryExpressionContext _localctx = new SubqueryExpressionContext(_ctx, getState());
    enterRule(_localctx, 92, RULE_subqueryExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(448);
      match(LP);
      setState(449);
      query(0);
      setState(450);
      match(RP);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ShowCommandContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public ShowCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_showCommand; }
   
    @SuppressWarnings("this-escape")
    public ShowCommandContext() { }
    public void copyFrom(ShowCommandContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ShowInfoContext extends ShowCommandContext {
    public TerminalNode SHOW() { return getToken(EsqlBaseParser.SHOW, 0); }
    public TerminalNode INFO() { return getToken(EsqlBaseParser.INFO, 0); }
    @SuppressWarnings("this-escape")
    public ShowInfoContext(ShowCommandContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterShowInfo(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitShowInfo(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitShowInfo(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ShowCommandContext showCommand() throws RecognitionException {
    ShowCommandContext _localctx = new ShowCommandContext(_ctx, getState());
    enterRule(_localctx, 94, RULE_showCommand);
    try {
      _localctx = new ShowInfoContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(452);
      match(SHOW);
      setState(453);
      match(INFO);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class EnrichCommandContext extends ParserRuleContext {
    public EnrichPolicyNameContext policyName;
    public QualifiedNamePatternContext matchField;
    public TerminalNode ENRICH() { return getToken(EsqlBaseParser.ENRICH, 0); }
    public EnrichPolicyNameContext enrichPolicyName() {
      return getRuleContext(EnrichPolicyNameContext.class,0);
    }
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
    public TerminalNode WITH() { return getToken(EsqlBaseParser.WITH, 0); }
    public List<EnrichWithClauseContext> enrichWithClause() {
      return getRuleContexts(EnrichWithClauseContext.class);
    }
    public EnrichWithClauseContext enrichWithClause(int i) {
      return getRuleContext(EnrichWithClauseContext.class,i);
    }
    public QualifiedNamePatternContext qualifiedNamePattern() {
      return getRuleContext(QualifiedNamePatternContext.class,0);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public EnrichCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_enrichCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterEnrichCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitEnrichCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitEnrichCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EnrichCommandContext enrichCommand() throws RecognitionException {
    EnrichCommandContext _localctx = new EnrichCommandContext(_ctx, getState());
    enterRule(_localctx, 96, RULE_enrichCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(455);
      match(ENRICH);
      setState(456);
      ((EnrichCommandContext)_localctx).policyName = enrichPolicyName();
      setState(459);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
      case 1:
        {
        setState(457);
        match(ON);
        setState(458);
        ((EnrichCommandContext)_localctx).matchField = qualifiedNamePattern();
        }
        break;
      }
      setState(470);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
      case 1:
        {
        setState(461);
        match(WITH);
        setState(462);
        enrichWithClause();
        setState(467);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,31,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(463);
            match(COMMA);
            setState(464);
            enrichWithClause();
            }
            } 
          }
          setState(469);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,31,_ctx);
        }
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class EnrichPolicyNameContext extends ParserRuleContext {
    public TerminalNode ENRICH_POLICY_NAME() { return getToken(EsqlBaseParser.ENRICH_POLICY_NAME, 0); }
    public TerminalNode QUOTED_STRING() { return getToken(EsqlBaseParser.QUOTED_STRING, 0); }
    @SuppressWarnings("this-escape")
    public EnrichPolicyNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_enrichPolicyName; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterEnrichPolicyName(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitEnrichPolicyName(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitEnrichPolicyName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EnrichPolicyNameContext enrichPolicyName() throws RecognitionException {
    EnrichPolicyNameContext _localctx = new EnrichPolicyNameContext(_ctx, getState());
    enterRule(_localctx, 98, RULE_enrichPolicyName);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(472);
      _la = _input.LA(1);
      if ( !(_la==ENRICH_POLICY_NAME || _la==QUOTED_STRING) ) {
      _errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class EnrichWithClauseContext extends ParserRuleContext {
    public QualifiedNamePatternContext newName;
    public QualifiedNamePatternContext enrichField;
    public List<QualifiedNamePatternContext> qualifiedNamePattern() {
      return getRuleContexts(QualifiedNamePatternContext.class);
    }
    public QualifiedNamePatternContext qualifiedNamePattern(int i) {
      return getRuleContext(QualifiedNamePatternContext.class,i);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    @SuppressWarnings("this-escape")
    public EnrichWithClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_enrichWithClause; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterEnrichWithClause(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitEnrichWithClause(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitEnrichWithClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EnrichWithClauseContext enrichWithClause() throws RecognitionException {
    EnrichWithClauseContext _localctx = new EnrichWithClauseContext(_ctx, getState());
    enterRule(_localctx, 100, RULE_enrichWithClause);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(477);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,33,_ctx) ) {
      case 1:
        {
        setState(474);
        ((EnrichWithClauseContext)_localctx).newName = qualifiedNamePattern();
        setState(475);
        match(ASSIGN);
        }
        break;
      }
      setState(479);
      ((EnrichWithClauseContext)_localctx).enrichField = qualifiedNamePattern();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class SampleCommandContext extends ParserRuleContext {
    public ConstantContext probability;
    public TerminalNode SAMPLE() { return getToken(EsqlBaseParser.SAMPLE, 0); }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public SampleCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_sampleCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSampleCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSampleCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSampleCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SampleCommandContext sampleCommand() throws RecognitionException {
    SampleCommandContext _localctx = new SampleCommandContext(_ctx, getState());
    enterRule(_localctx, 102, RULE_sampleCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(481);
      match(SAMPLE);
      setState(482);
      ((SampleCommandContext)_localctx).probability = constant();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ChangePointCommandContext extends ParserRuleContext {
    public QualifiedNameContext value;
    public QualifiedNameContext key;
    public QualifiedNameContext targetType;
    public QualifiedNameContext targetPvalue;
    public TerminalNode CHANGE_POINT() { return getToken(EsqlBaseParser.CHANGE_POINT, 0); }
    public List<QualifiedNameContext> qualifiedName() {
      return getRuleContexts(QualifiedNameContext.class);
    }
    public QualifiedNameContext qualifiedName(int i) {
      return getRuleContext(QualifiedNameContext.class,i);
    }
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
    public TerminalNode AS() { return getToken(EsqlBaseParser.AS, 0); }
    public TerminalNode COMMA() { return getToken(EsqlBaseParser.COMMA, 0); }
    @SuppressWarnings("this-escape")
    public ChangePointCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_changePointCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterChangePointCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitChangePointCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitChangePointCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ChangePointCommandContext changePointCommand() throws RecognitionException {
    ChangePointCommandContext _localctx = new ChangePointCommandContext(_ctx, getState());
    enterRule(_localctx, 104, RULE_changePointCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(484);
      match(CHANGE_POINT);
      setState(485);
      ((ChangePointCommandContext)_localctx).value = qualifiedName();
      setState(488);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
      case 1:
        {
        setState(486);
        match(ON);
        setState(487);
        ((ChangePointCommandContext)_localctx).key = qualifiedName();
        }
        break;
      }
      setState(495);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,35,_ctx) ) {
      case 1:
        {
        setState(490);
        match(AS);
        setState(491);
        ((ChangePointCommandContext)_localctx).targetType = qualifiedName();
        setState(492);
        match(COMMA);
        setState(493);
        ((ChangePointCommandContext)_localctx).targetPvalue = qualifiedName();
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ForkCommandContext extends ParserRuleContext {
    public TerminalNode FORK() { return getToken(EsqlBaseParser.FORK, 0); }
    public ForkSubQueriesContext forkSubQueries() {
      return getRuleContext(ForkSubQueriesContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ForkCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_forkCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterForkCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitForkCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitForkCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ForkCommandContext forkCommand() throws RecognitionException {
    ForkCommandContext _localctx = new ForkCommandContext(_ctx, getState());
    enterRule(_localctx, 106, RULE_forkCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(497);
      match(FORK);
      setState(498);
      forkSubQueries();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ForkSubQueriesContext extends ParserRuleContext {
    public List<ForkSubQueryContext> forkSubQuery() {
      return getRuleContexts(ForkSubQueryContext.class);
    }
    public ForkSubQueryContext forkSubQuery(int i) {
      return getRuleContext(ForkSubQueryContext.class,i);
    }
    @SuppressWarnings("this-escape")
    public ForkSubQueriesContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_forkSubQueries; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterForkSubQueries(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitForkSubQueries(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitForkSubQueries(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ForkSubQueriesContext forkSubQueries() throws RecognitionException {
    ForkSubQueriesContext _localctx = new ForkSubQueriesContext(_ctx, getState());
    enterRule(_localctx, 108, RULE_forkSubQueries);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(501); 
      _errHandler.sync(this);
      _alt = 1;
      do {
        switch (_alt) {
        case 1:
          {
          {
          setState(500);
          forkSubQuery();
          }
          }
          break;
        default:
          throw new NoViableAltException(this);
        }
        setState(503); 
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,36,_ctx);
      } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ForkSubQueryContext extends ParserRuleContext {
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public ForkSubQueryCommandContext forkSubQueryCommand() {
      return getRuleContext(ForkSubQueryCommandContext.class,0);
    }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    @SuppressWarnings("this-escape")
    public ForkSubQueryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_forkSubQuery; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterForkSubQuery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitForkSubQuery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitForkSubQuery(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ForkSubQueryContext forkSubQuery() throws RecognitionException {
    ForkSubQueryContext _localctx = new ForkSubQueryContext(_ctx, getState());
    enterRule(_localctx, 110, RULE_forkSubQuery);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(505);
      match(LP);
      setState(506);
      forkSubQueryCommand(0);
      setState(507);
      match(RP);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ForkSubQueryCommandContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public ForkSubQueryCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_forkSubQueryCommand; }
   
    @SuppressWarnings("this-escape")
    public ForkSubQueryCommandContext() { }
    public void copyFrom(ForkSubQueryCommandContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class SingleForkSubQueryCommandContext extends ForkSubQueryCommandContext {
    public ForkSubQueryProcessingCommandContext forkSubQueryProcessingCommand() {
      return getRuleContext(ForkSubQueryProcessingCommandContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public SingleForkSubQueryCommandContext(ForkSubQueryCommandContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSingleForkSubQueryCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSingleForkSubQueryCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSingleForkSubQueryCommand(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class CompositeForkSubQueryContext extends ForkSubQueryCommandContext {
    public ForkSubQueryCommandContext forkSubQueryCommand() {
      return getRuleContext(ForkSubQueryCommandContext.class,0);
    }
    public TerminalNode PIPE() { return getToken(EsqlBaseParser.PIPE, 0); }
    public ForkSubQueryProcessingCommandContext forkSubQueryProcessingCommand() {
      return getRuleContext(ForkSubQueryProcessingCommandContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public CompositeForkSubQueryContext(ForkSubQueryCommandContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterCompositeForkSubQuery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitCompositeForkSubQuery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitCompositeForkSubQuery(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ForkSubQueryCommandContext forkSubQueryCommand() throws RecognitionException {
    return forkSubQueryCommand(0);
  }

  private ForkSubQueryCommandContext forkSubQueryCommand(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    ForkSubQueryCommandContext _localctx = new ForkSubQueryCommandContext(_ctx, _parentState);
    ForkSubQueryCommandContext _prevctx = _localctx;
    int _startState = 112;
    enterRecursionRule(_localctx, 112, RULE_forkSubQueryCommand, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      {
      _localctx = new SingleForkSubQueryCommandContext(_localctx);
      _ctx = _localctx;
      _prevctx = _localctx;

      setState(510);
      forkSubQueryProcessingCommand();
      }
      _ctx.stop = _input.LT(-1);
      setState(517);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,37,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          {
          _localctx = new CompositeForkSubQueryContext(new ForkSubQueryCommandContext(_parentctx, _parentState));
          pushNewRecursionContext(_localctx, _startState, RULE_forkSubQueryCommand);
          setState(512);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(513);
          match(PIPE);
          setState(514);
          forkSubQueryProcessingCommand();
          }
          } 
        }
        setState(519);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,37,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ForkSubQueryProcessingCommandContext extends ParserRuleContext {
    public ProcessingCommandContext processingCommand() {
      return getRuleContext(ProcessingCommandContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ForkSubQueryProcessingCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_forkSubQueryProcessingCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterForkSubQueryProcessingCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitForkSubQueryProcessingCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitForkSubQueryProcessingCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ForkSubQueryProcessingCommandContext forkSubQueryProcessingCommand() throws RecognitionException {
    ForkSubQueryProcessingCommandContext _localctx = new ForkSubQueryProcessingCommandContext(_ctx, getState());
    enterRule(_localctx, 114, RULE_forkSubQueryProcessingCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(520);
      processingCommand();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class CompletionCommandContext extends ParserRuleContext {
    public QualifiedNameContext targetField;
    public PrimaryExpressionContext prompt;
    public TerminalNode COMPLETION() { return getToken(EsqlBaseParser.COMPLETION, 0); }
    public CommandNamedParametersContext commandNamedParameters() {
      return getRuleContext(CommandNamedParametersContext.class,0);
    }
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public CompletionCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_completionCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterCompletionCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitCompletionCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitCompletionCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CompletionCommandContext completionCommand() throws RecognitionException {
    CompletionCommandContext _localctx = new CompletionCommandContext(_ctx, getState());
    enterRule(_localctx, 116, RULE_completionCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(522);
      match(COMPLETION);
      setState(526);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,38,_ctx) ) {
      case 1:
        {
        setState(523);
        ((CompletionCommandContext)_localctx).targetField = qualifiedName();
        setState(524);
        match(ASSIGN);
        }
        break;
      }
      setState(528);
      ((CompletionCommandContext)_localctx).prompt = primaryExpression(0);
      setState(529);
      commandNamedParameters();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class LookupCommandContext extends ParserRuleContext {
    public IndexPatternContext tableName;
    public QualifiedNamePatternsContext matchFields;
    public TerminalNode DEV_LOOKUP() { return getToken(EsqlBaseParser.DEV_LOOKUP, 0); }
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
    public IndexPatternContext indexPattern() {
      return getRuleContext(IndexPatternContext.class,0);
    }
    public QualifiedNamePatternsContext qualifiedNamePatterns() {
      return getRuleContext(QualifiedNamePatternsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public LookupCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_lookupCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLookupCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLookupCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLookupCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LookupCommandContext lookupCommand() throws RecognitionException {
    LookupCommandContext _localctx = new LookupCommandContext(_ctx, getState());
    enterRule(_localctx, 118, RULE_lookupCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(531);
      match(DEV_LOOKUP);
      setState(532);
      ((LookupCommandContext)_localctx).tableName = indexPattern();
      setState(533);
      match(ON);
      setState(534);
      ((LookupCommandContext)_localctx).matchFields = qualifiedNamePatterns();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class InlinestatsCommandContext extends ParserRuleContext {
    public AggFieldsContext stats;
    public FieldsContext grouping;
    public TerminalNode DEV_INLINESTATS() { return getToken(EsqlBaseParser.DEV_INLINESTATS, 0); }
    public AggFieldsContext aggFields() {
      return getRuleContext(AggFieldsContext.class,0);
    }
    public TerminalNode BY() { return getToken(EsqlBaseParser.BY, 0); }
    public FieldsContext fields() {
      return getRuleContext(FieldsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public InlinestatsCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_inlinestatsCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInlinestatsCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInlinestatsCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInlinestatsCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final InlinestatsCommandContext inlinestatsCommand() throws RecognitionException {
    InlinestatsCommandContext _localctx = new InlinestatsCommandContext(_ctx, getState());
    enterRule(_localctx, 120, RULE_inlinestatsCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(536);
      match(DEV_INLINESTATS);
      setState(537);
      ((InlinestatsCommandContext)_localctx).stats = aggFields();
      setState(540);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,39,_ctx) ) {
      case 1:
        {
        setState(538);
        match(BY);
        setState(539);
        ((InlinestatsCommandContext)_localctx).grouping = fields();
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class InsistCommandContext extends ParserRuleContext {
    public TerminalNode DEV_INSIST() { return getToken(EsqlBaseParser.DEV_INSIST, 0); }
    public QualifiedNamePatternsContext qualifiedNamePatterns() {
      return getRuleContext(QualifiedNamePatternsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public InsistCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_insistCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInsistCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInsistCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInsistCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final InsistCommandContext insistCommand() throws RecognitionException {
    InsistCommandContext _localctx = new InsistCommandContext(_ctx, getState());
    enterRule(_localctx, 122, RULE_insistCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(542);
      match(DEV_INSIST);
      setState(543);
      qualifiedNamePatterns();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class FuseCommandContext extends ParserRuleContext {
    public TerminalNode DEV_FUSE() { return getToken(EsqlBaseParser.DEV_FUSE, 0); }
    @SuppressWarnings("this-escape")
    public FuseCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_fuseCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFuseCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFuseCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFuseCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FuseCommandContext fuseCommand() throws RecognitionException {
    FuseCommandContext _localctx = new FuseCommandContext(_ctx, getState());
    enterRule(_localctx, 124, RULE_fuseCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(545);
      match(DEV_FUSE);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class RerankCommandContext extends ParserRuleContext {
    public QualifiedNameContext targetField;
    public ConstantContext queryText;
    public TerminalNode DEV_RERANK() { return getToken(EsqlBaseParser.DEV_RERANK, 0); }
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
    public RerankFieldsContext rerankFields() {
      return getRuleContext(RerankFieldsContext.class,0);
    }
    public CommandNamedParametersContext commandNamedParameters() {
      return getRuleContext(CommandNamedParametersContext.class,0);
    }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public RerankCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_rerankCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRerankCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRerankCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRerankCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RerankCommandContext rerankCommand() throws RecognitionException {
    RerankCommandContext _localctx = new RerankCommandContext(_ctx, getState());
    enterRule(_localctx, 126, RULE_rerankCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(547);
      match(DEV_RERANK);
      setState(551);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,40,_ctx) ) {
      case 1:
        {
        setState(548);
        ((RerankCommandContext)_localctx).targetField = qualifiedName();
        setState(549);
        match(ASSIGN);
        }
        break;
      }
      setState(553);
      ((RerankCommandContext)_localctx).queryText = constant();
      setState(554);
      match(ON);
      setState(555);
      rerankFields();
      setState(556);
      commandNamedParameters();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class BooleanExpressionContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_booleanExpression; }
   
    @SuppressWarnings("this-escape")
    public BooleanExpressionContext() { }
    public void copyFrom(BooleanExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class MatchExpressionContext extends BooleanExpressionContext {
    public MatchBooleanExpressionContext matchBooleanExpression() {
      return getRuleContext(MatchBooleanExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public MatchExpressionContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMatchExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMatchExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMatchExpression(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class LogicalNotContext extends BooleanExpressionContext {
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public LogicalNotContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLogicalNot(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLogicalNot(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLogicalNot(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class BooleanDefaultContext extends BooleanExpressionContext {
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public BooleanDefaultContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterBooleanDefault(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitBooleanDefault(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitBooleanDefault(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class IsNullContext extends BooleanExpressionContext {
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public TerminalNode IS() { return getToken(EsqlBaseParser.IS, 0); }
    public TerminalNode NULL() { return getToken(EsqlBaseParser.NULL, 0); }
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    @SuppressWarnings("this-escape")
    public IsNullContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIsNull(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIsNull(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIsNull(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class RegexExpressionContext extends BooleanExpressionContext {
    public RegexBooleanExpressionContext regexBooleanExpression() {
      return getRuleContext(RegexBooleanExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public RegexExpressionContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRegexExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRegexExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRegexExpression(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class LogicalInContext extends BooleanExpressionContext {
    public List<ValueExpressionContext> valueExpression() {
      return getRuleContexts(ValueExpressionContext.class);
    }
    public ValueExpressionContext valueExpression(int i) {
      return getRuleContext(ValueExpressionContext.class,i);
    }
    public TerminalNode IN() { return getToken(EsqlBaseParser.IN, 0); }
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public LogicalInContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLogicalIn(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLogicalIn(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLogicalIn(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class LogicalBinaryContext extends BooleanExpressionContext {
    public BooleanExpressionContext left;
    public Token operator;
    public BooleanExpressionContext right;
    public List<BooleanExpressionContext> booleanExpression() {
      return getRuleContexts(BooleanExpressionContext.class);
    }
    public BooleanExpressionContext booleanExpression(int i) {
      return getRuleContext(BooleanExpressionContext.class,i);
    }
    public TerminalNode AND() { return getToken(EsqlBaseParser.AND, 0); }
    public TerminalNode OR() { return getToken(EsqlBaseParser.OR, 0); }
    @SuppressWarnings("this-escape")
    public LogicalBinaryContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLogicalBinary(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLogicalBinary(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLogicalBinary(this);
      else return visitor.visitChildren(this);
    }
  }

  public final BooleanExpressionContext booleanExpression() throws RecognitionException {
    return booleanExpression(0);
  }

  private BooleanExpressionContext booleanExpression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    BooleanExpressionContext _localctx = new BooleanExpressionContext(_ctx, _parentState);
    BooleanExpressionContext _prevctx = _localctx;
    int _startState = 128;
    enterRecursionRule(_localctx, 128, RULE_booleanExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(587);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,44,_ctx) ) {
      case 1:
        {
        _localctx = new LogicalNotContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(559);
        match(NOT);
        setState(560);
        booleanExpression(8);
        }
        break;
      case 2:
        {
        _localctx = new BooleanDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(561);
        valueExpression();
        }
        break;
      case 3:
        {
        _localctx = new RegexExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(562);
        regexBooleanExpression();
        }
        break;
      case 4:
        {
        _localctx = new LogicalInContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(563);
        valueExpression();
        setState(565);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(564);
          match(NOT);
          }
        }

        setState(567);
        match(IN);
        setState(568);
        match(LP);
        setState(569);
        valueExpression();
        setState(574);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(570);
          match(COMMA);
          setState(571);
          valueExpression();
          }
          }
          setState(576);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(577);
        match(RP);
        }
        break;
      case 5:
        {
        _localctx = new IsNullContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(579);
        valueExpression();
        setState(580);
        match(IS);
        setState(582);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(581);
          match(NOT);
          }
        }

        setState(584);
        match(NULL);
        }
        break;
      case 6:
        {
        _localctx = new MatchExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(586);
        matchBooleanExpression();
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(597);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,46,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(595);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,45,_ctx) ) {
          case 1:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(589);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(590);
            ((LogicalBinaryContext)_localctx).operator = match(AND);
            setState(591);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(6);
            }
            break;
          case 2:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(592);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(593);
            ((LogicalBinaryContext)_localctx).operator = match(OR);
            setState(594);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(5);
            }
            break;
          }
          } 
        }
        setState(599);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,46,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class RegexBooleanExpressionContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public RegexBooleanExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_regexBooleanExpression; }
   
    @SuppressWarnings("this-escape")
    public RegexBooleanExpressionContext() { }
    public void copyFrom(RegexBooleanExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class LikeExpressionContext extends RegexBooleanExpressionContext {
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public TerminalNode LIKE() { return getToken(EsqlBaseParser.LIKE, 0); }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    @SuppressWarnings("this-escape")
    public LikeExpressionContext(RegexBooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLikeExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLikeExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLikeExpression(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class LikeListExpressionContext extends RegexBooleanExpressionContext {
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public TerminalNode LIKE() { return getToken(EsqlBaseParser.LIKE, 0); }
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public List<StringContext> string() {
      return getRuleContexts(StringContext.class);
    }
    public StringContext string(int i) {
      return getRuleContext(StringContext.class,i);
    }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public LikeListExpressionContext(RegexBooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLikeListExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLikeListExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLikeListExpression(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class RlikeExpressionContext extends RegexBooleanExpressionContext {
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public TerminalNode RLIKE() { return getToken(EsqlBaseParser.RLIKE, 0); }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    @SuppressWarnings("this-escape")
    public RlikeExpressionContext(RegexBooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRlikeExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRlikeExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRlikeExpression(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class RlikeListExpressionContext extends RegexBooleanExpressionContext {
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public TerminalNode RLIKE() { return getToken(EsqlBaseParser.RLIKE, 0); }
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public List<StringContext> string() {
      return getRuleContexts(StringContext.class);
    }
    public StringContext string(int i) {
      return getRuleContext(StringContext.class,i);
    }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public RlikeListExpressionContext(RegexBooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRlikeListExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRlikeListExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRlikeListExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RegexBooleanExpressionContext regexBooleanExpression() throws RecognitionException {
    RegexBooleanExpressionContext _localctx = new RegexBooleanExpressionContext(_ctx, getState());
    enterRule(_localctx, 130, RULE_regexBooleanExpression);
    int _la;
    try {
      setState(646);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,53,_ctx) ) {
      case 1:
        _localctx = new LikeExpressionContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(600);
        valueExpression();
        setState(602);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(601);
          match(NOT);
          }
        }

        setState(604);
        match(LIKE);
        setState(605);
        string();
        }
        break;
      case 2:
        _localctx = new RlikeExpressionContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(607);
        valueExpression();
        setState(609);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(608);
          match(NOT);
          }
        }

        setState(611);
        match(RLIKE);
        setState(612);
        string();
        }
        break;
      case 3:
        _localctx = new LikeListExpressionContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(614);
        valueExpression();
        setState(616);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(615);
          match(NOT);
          }
        }

        setState(618);
        match(LIKE);
        setState(619);
        match(LP);
        setState(620);
        string();
        setState(625);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(621);
          match(COMMA);
          setState(622);
          string();
          }
          }
          setState(627);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(628);
        match(RP);
        }
        break;
      case 4:
        _localctx = new RlikeListExpressionContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(630);
        valueExpression();
        setState(632);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(631);
          match(NOT);
          }
        }

        setState(634);
        match(RLIKE);
        setState(635);
        match(LP);
        setState(636);
        string();
        setState(641);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(637);
          match(COMMA);
          setState(638);
          string();
          }
          }
          setState(643);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(644);
        match(RP);
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class MatchBooleanExpressionContext extends ParserRuleContext {
    public QualifiedNameContext fieldExp;
    public DataTypeContext fieldType;
    public ConstantContext matchQuery;
    public TerminalNode COLON() { return getToken(EsqlBaseParser.COLON, 0); }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    public TerminalNode CAST_OP() { return getToken(EsqlBaseParser.CAST_OP, 0); }
    public DataTypeContext dataType() {
      return getRuleContext(DataTypeContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public MatchBooleanExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_matchBooleanExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMatchBooleanExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMatchBooleanExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMatchBooleanExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MatchBooleanExpressionContext matchBooleanExpression() throws RecognitionException {
    MatchBooleanExpressionContext _localctx = new MatchBooleanExpressionContext(_ctx, getState());
    enterRule(_localctx, 132, RULE_matchBooleanExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(648);
      ((MatchBooleanExpressionContext)_localctx).fieldExp = qualifiedName();
      setState(651);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==CAST_OP) {
        {
        setState(649);
        match(CAST_OP);
        setState(650);
        ((MatchBooleanExpressionContext)_localctx).fieldType = dataType();
        }
      }

      setState(653);
      match(COLON);
      setState(654);
      ((MatchBooleanExpressionContext)_localctx).matchQuery = constant();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ValueExpressionContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public ValueExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_valueExpression; }
   
    @SuppressWarnings("this-escape")
    public ValueExpressionContext() { }
    public void copyFrom(ValueExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ValueExpressionDefaultContext extends ValueExpressionContext {
    public OperatorExpressionContext operatorExpression() {
      return getRuleContext(OperatorExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ValueExpressionDefaultContext(ValueExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterValueExpressionDefault(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitValueExpressionDefault(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitValueExpressionDefault(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ComparisonContext extends ValueExpressionContext {
    public OperatorExpressionContext left;
    public OperatorExpressionContext right;
    public ComparisonOperatorContext comparisonOperator() {
      return getRuleContext(ComparisonOperatorContext.class,0);
    }
    public List<OperatorExpressionContext> operatorExpression() {
      return getRuleContexts(OperatorExpressionContext.class);
    }
    public OperatorExpressionContext operatorExpression(int i) {
      return getRuleContext(OperatorExpressionContext.class,i);
    }
    @SuppressWarnings("this-escape")
    public ComparisonContext(ValueExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterComparison(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitComparison(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitComparison(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ValueExpressionContext valueExpression() throws RecognitionException {
    ValueExpressionContext _localctx = new ValueExpressionContext(_ctx, getState());
    enterRule(_localctx, 134, RULE_valueExpression);
    try {
      setState(661);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,55,_ctx) ) {
      case 1:
        _localctx = new ValueExpressionDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(656);
        operatorExpression(0);
        }
        break;
      case 2:
        _localctx = new ComparisonContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(657);
        ((ComparisonContext)_localctx).left = operatorExpression(0);
        setState(658);
        comparisonOperator();
        setState(659);
        ((ComparisonContext)_localctx).right = operatorExpression(0);
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class OperatorExpressionContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public OperatorExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_operatorExpression; }
   
    @SuppressWarnings("this-escape")
    public OperatorExpressionContext() { }
    public void copyFrom(OperatorExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class OperatorExpressionDefaultContext extends OperatorExpressionContext {
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public OperatorExpressionDefaultContext(OperatorExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterOperatorExpressionDefault(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitOperatorExpressionDefault(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitOperatorExpressionDefault(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ArithmeticBinaryContext extends OperatorExpressionContext {
    public OperatorExpressionContext left;
    public Token operator;
    public OperatorExpressionContext right;
    public List<OperatorExpressionContext> operatorExpression() {
      return getRuleContexts(OperatorExpressionContext.class);
    }
    public OperatorExpressionContext operatorExpression(int i) {
      return getRuleContext(OperatorExpressionContext.class,i);
    }
    public TerminalNode ASTERISK() { return getToken(EsqlBaseParser.ASTERISK, 0); }
    public TerminalNode SLASH() { return getToken(EsqlBaseParser.SLASH, 0); }
    public TerminalNode PERCENT() { return getToken(EsqlBaseParser.PERCENT, 0); }
    public TerminalNode PLUS() { return getToken(EsqlBaseParser.PLUS, 0); }
    public TerminalNode MINUS() { return getToken(EsqlBaseParser.MINUS, 0); }
    @SuppressWarnings("this-escape")
    public ArithmeticBinaryContext(OperatorExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterArithmeticBinary(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitArithmeticBinary(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitArithmeticBinary(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ArithmeticUnaryContext extends OperatorExpressionContext {
    public Token operator;
    public OperatorExpressionContext operatorExpression() {
      return getRuleContext(OperatorExpressionContext.class,0);
    }
    public TerminalNode MINUS() { return getToken(EsqlBaseParser.MINUS, 0); }
    public TerminalNode PLUS() { return getToken(EsqlBaseParser.PLUS, 0); }
    @SuppressWarnings("this-escape")
    public ArithmeticUnaryContext(OperatorExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterArithmeticUnary(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitArithmeticUnary(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitArithmeticUnary(this);
      else return visitor.visitChildren(this);
    }
  }

  public final OperatorExpressionContext operatorExpression() throws RecognitionException {
    return operatorExpression(0);
  }

  private OperatorExpressionContext operatorExpression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    OperatorExpressionContext _localctx = new OperatorExpressionContext(_ctx, _parentState);
    OperatorExpressionContext _prevctx = _localctx;
    int _startState = 136;
    enterRecursionRule(_localctx, 136, RULE_operatorExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(667);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,56,_ctx) ) {
      case 1:
        {
        _localctx = new OperatorExpressionDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(664);
        primaryExpression(0);
        }
        break;
      case 2:
        {
        _localctx = new ArithmeticUnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(665);
        ((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==PLUS || _la==MINUS) ) {
          ((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        setState(666);
        operatorExpression(3);
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(677);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,58,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(675);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,57,_ctx) ) {
          case 1:
            {
            _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
            setState(669);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(670);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(((((_la - 89)) & ~0x3f) == 0 && ((1L << (_la - 89)) & 7L) != 0)) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            }
            else {
              if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(671);
            ((ArithmeticBinaryContext)_localctx).right = operatorExpression(3);
            }
            break;
          case 2:
            {
            _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
            setState(672);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(673);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(_la==PLUS || _la==MINUS) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            }
            else {
              if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(674);
            ((ArithmeticBinaryContext)_localctx).right = operatorExpression(2);
            }
            break;
          }
          } 
        }
        setState(679);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,58,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class PrimaryExpressionContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_primaryExpression; }
   
    @SuppressWarnings("this-escape")
    public PrimaryExpressionContext() { }
    public void copyFrom(PrimaryExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class DereferenceContext extends PrimaryExpressionContext {
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public DereferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDereference(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDereference(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDereference(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class InlineCastContext extends PrimaryExpressionContext {
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    public TerminalNode CAST_OP() { return getToken(EsqlBaseParser.CAST_OP, 0); }
    public DataTypeContext dataType() {
      return getRuleContext(DataTypeContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public InlineCastContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInlineCast(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInlineCast(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInlineCast(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ConstantDefaultContext extends PrimaryExpressionContext {
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ConstantDefaultContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterConstantDefault(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitConstantDefault(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitConstantDefault(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    @SuppressWarnings("this-escape")
    public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterParenthesizedExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitParenthesizedExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitParenthesizedExpression(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class FunctionContext extends PrimaryExpressionContext {
    public FunctionExpressionContext functionExpression() {
      return getRuleContext(FunctionExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public FunctionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFunction(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFunction(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFunction(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
    return primaryExpression(0);
  }

  private PrimaryExpressionContext primaryExpression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, _parentState);
    PrimaryExpressionContext _prevctx = _localctx;
    int _startState = 138;
    enterRecursionRule(_localctx, 138, RULE_primaryExpression, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(688);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,59,_ctx) ) {
      case 1:
        {
        _localctx = new ConstantDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(681);
        constant();
        }
        break;
      case 2:
        {
        _localctx = new DereferenceContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(682);
        qualifiedName();
        }
        break;
      case 3:
        {
        _localctx = new FunctionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(683);
        functionExpression();
        }
        break;
      case 4:
        {
        _localctx = new ParenthesizedExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(684);
        match(LP);
        setState(685);
        booleanExpression(0);
        setState(686);
        match(RP);
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(695);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,60,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          {
          _localctx = new InlineCastContext(new PrimaryExpressionContext(_parentctx, _parentState));
          pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
          setState(690);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(691);
          match(CAST_OP);
          setState(692);
          dataType();
          }
          } 
        }
        setState(697);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,60,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class FunctionExpressionContext extends ParserRuleContext {
    public FunctionNameContext functionName() {
      return getRuleContext(FunctionNameContext.class,0);
    }
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    public TerminalNode ASTERISK() { return getToken(EsqlBaseParser.ASTERISK, 0); }
    public List<BooleanExpressionContext> booleanExpression() {
      return getRuleContexts(BooleanExpressionContext.class);
    }
    public BooleanExpressionContext booleanExpression(int i) {
      return getRuleContext(BooleanExpressionContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    public MapExpressionContext mapExpression() {
      return getRuleContext(MapExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public FunctionExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_functionExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFunctionExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFunctionExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFunctionExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FunctionExpressionContext functionExpression() throws RecognitionException {
    FunctionExpressionContext _localctx = new FunctionExpressionContext(_ctx, getState());
    enterRule(_localctx, 140, RULE_functionExpression);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(698);
      functionName();
      setState(699);
      match(LP);
      setState(713);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case ASTERISK:
        {
        setState(700);
        match(ASTERISK);
        }
        break;
      case QUOTED_STRING:
      case INTEGER_LITERAL:
      case DECIMAL_LITERAL:
      case FALSE:
      case NOT:
      case NULL:
      case PARAM:
      case TRUE:
      case PLUS:
      case MINUS:
      case DOUBLE_PARAMS:
      case NAMED_OR_POSITIONAL_PARAM:
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
      case OPENING_BRACKET:
      case LP:
      case UNQUOTED_IDENTIFIER:
      case QUOTED_IDENTIFIER:
        {
        {
        setState(701);
        booleanExpression(0);
        setState(706);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,61,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(702);
            match(COMMA);
            setState(703);
            booleanExpression(0);
            }
            } 
          }
          setState(708);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,61,_ctx);
        }
        setState(711);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==COMMA) {
          {
          setState(709);
          match(COMMA);
          setState(710);
          mapExpression();
          }
        }

        }
        }
        break;
      case RP:
        break;
      default:
        break;
      }
      setState(715);
      match(RP);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class FunctionNameContext extends ParserRuleContext {
    public IdentifierOrParameterContext identifierOrParameter() {
      return getRuleContext(IdentifierOrParameterContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public FunctionNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_functionName; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFunctionName(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFunctionName(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFunctionName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FunctionNameContext functionName() throws RecognitionException {
    FunctionNameContext _localctx = new FunctionNameContext(_ctx, getState());
    enterRule(_localctx, 142, RULE_functionName);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(717);
      identifierOrParameter();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class MapExpressionContext extends ParserRuleContext {
    public TerminalNode LEFT_BRACES() { return getToken(EsqlBaseParser.LEFT_BRACES, 0); }
    public TerminalNode RIGHT_BRACES() { return getToken(EsqlBaseParser.RIGHT_BRACES, 0); }
    public List<EntryExpressionContext> entryExpression() {
      return getRuleContexts(EntryExpressionContext.class);
    }
    public EntryExpressionContext entryExpression(int i) {
      return getRuleContext(EntryExpressionContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public MapExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_mapExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMapExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMapExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMapExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MapExpressionContext mapExpression() throws RecognitionException {
    MapExpressionContext _localctx = new MapExpressionContext(_ctx, getState());
    enterRule(_localctx, 144, RULE_mapExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(719);
      match(LEFT_BRACES);
      setState(728);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==QUOTED_STRING) {
        {
        setState(720);
        entryExpression();
        setState(725);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(721);
          match(COMMA);
          setState(722);
          entryExpression();
          }
          }
          setState(727);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(730);
      match(RIGHT_BRACES);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class EntryExpressionContext extends ParserRuleContext {
    public StringContext key;
    public ConstantContext value;
    public TerminalNode COLON() { return getToken(EsqlBaseParser.COLON, 0); }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public EntryExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_entryExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterEntryExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitEntryExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitEntryExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EntryExpressionContext entryExpression() throws RecognitionException {
    EntryExpressionContext _localctx = new EntryExpressionContext(_ctx, getState());
    enterRule(_localctx, 146, RULE_entryExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(732);
      ((EntryExpressionContext)_localctx).key = string();
      setState(733);
      match(COLON);
      setState(734);
      ((EntryExpressionContext)_localctx).value = constant();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ConstantContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public ConstantContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_constant; }
   
    @SuppressWarnings("this-escape")
    public ConstantContext() { }
    public void copyFrom(ConstantContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class BooleanArrayLiteralContext extends ConstantContext {
    public TerminalNode OPENING_BRACKET() { return getToken(EsqlBaseParser.OPENING_BRACKET, 0); }
    public List<BooleanValueContext> booleanValue() {
      return getRuleContexts(BooleanValueContext.class);
    }
    public BooleanValueContext booleanValue(int i) {
      return getRuleContext(BooleanValueContext.class,i);
    }
    public TerminalNode CLOSING_BRACKET() { return getToken(EsqlBaseParser.CLOSING_BRACKET, 0); }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public BooleanArrayLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterBooleanArrayLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitBooleanArrayLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitBooleanArrayLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class DecimalLiteralContext extends ConstantContext {
    public DecimalValueContext decimalValue() {
      return getRuleContext(DecimalValueContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public DecimalLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDecimalLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDecimalLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDecimalLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class NullLiteralContext extends ConstantContext {
    public TerminalNode NULL() { return getToken(EsqlBaseParser.NULL, 0); }
    @SuppressWarnings("this-escape")
    public NullLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterNullLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitNullLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitNullLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class QualifiedIntegerLiteralContext extends ConstantContext {
    public IntegerValueContext integerValue() {
      return getRuleContext(IntegerValueContext.class,0);
    }
    public TerminalNode UNQUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.UNQUOTED_IDENTIFIER, 0); }
    @SuppressWarnings("this-escape")
    public QualifiedIntegerLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterQualifiedIntegerLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitQualifiedIntegerLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitQualifiedIntegerLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class StringArrayLiteralContext extends ConstantContext {
    public TerminalNode OPENING_BRACKET() { return getToken(EsqlBaseParser.OPENING_BRACKET, 0); }
    public List<StringContext> string() {
      return getRuleContexts(StringContext.class);
    }
    public StringContext string(int i) {
      return getRuleContext(StringContext.class,i);
    }
    public TerminalNode CLOSING_BRACKET() { return getToken(EsqlBaseParser.CLOSING_BRACKET, 0); }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public StringArrayLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterStringArrayLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitStringArrayLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitStringArrayLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class InputParameterContext extends ConstantContext {
    public ParameterContext parameter() {
      return getRuleContext(ParameterContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public InputParameterContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInputParameter(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInputParameter(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInputParameter(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class StringLiteralContext extends ConstantContext {
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public StringLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterStringLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitStringLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitStringLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class NumericArrayLiteralContext extends ConstantContext {
    public TerminalNode OPENING_BRACKET() { return getToken(EsqlBaseParser.OPENING_BRACKET, 0); }
    public List<NumericValueContext> numericValue() {
      return getRuleContexts(NumericValueContext.class);
    }
    public NumericValueContext numericValue(int i) {
      return getRuleContext(NumericValueContext.class,i);
    }
    public TerminalNode CLOSING_BRACKET() { return getToken(EsqlBaseParser.CLOSING_BRACKET, 0); }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public NumericArrayLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterNumericArrayLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitNumericArrayLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitNumericArrayLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class IntegerLiteralContext extends ConstantContext {
    public IntegerValueContext integerValue() {
      return getRuleContext(IntegerValueContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public IntegerLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIntegerLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIntegerLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIntegerLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class BooleanLiteralContext extends ConstantContext {
    public BooleanValueContext booleanValue() {
      return getRuleContext(BooleanValueContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public BooleanLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterBooleanLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitBooleanLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitBooleanLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ConstantContext constant() throws RecognitionException {
    ConstantContext _localctx = new ConstantContext(_ctx, getState());
    enterRule(_localctx, 148, RULE_constant);
    int _la;
    try {
      setState(778);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,69,_ctx) ) {
      case 1:
        _localctx = new NullLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(736);
        match(NULL);
        }
        break;
      case 2:
        _localctx = new QualifiedIntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(737);
        integerValue();
        setState(738);
        match(UNQUOTED_IDENTIFIER);
        }
        break;
      case 3:
        _localctx = new DecimalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(740);
        decimalValue();
        }
        break;
      case 4:
        _localctx = new IntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(741);
        integerValue();
        }
        break;
      case 5:
        _localctx = new BooleanLiteralContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(742);
        booleanValue();
        }
        break;
      case 6:
        _localctx = new InputParameterContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(743);
        parameter();
        }
        break;
      case 7:
        _localctx = new StringLiteralContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(744);
        string();
        }
        break;
      case 8:
        _localctx = new NumericArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(745);
        match(OPENING_BRACKET);
        setState(746);
        numericValue();
        setState(751);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(747);
          match(COMMA);
          setState(748);
          numericValue();
          }
          }
          setState(753);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(754);
        match(CLOSING_BRACKET);
        }
        break;
      case 9:
        _localctx = new BooleanArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(756);
        match(OPENING_BRACKET);
        setState(757);
        booleanValue();
        setState(762);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(758);
          match(COMMA);
          setState(759);
          booleanValue();
          }
          }
          setState(764);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(765);
        match(CLOSING_BRACKET);
        }
        break;
      case 10:
        _localctx = new StringArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(767);
        match(OPENING_BRACKET);
        setState(768);
        string();
        setState(773);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(769);
          match(COMMA);
          setState(770);
          string();
          }
          }
          setState(775);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(776);
        match(CLOSING_BRACKET);
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class BooleanValueContext extends ParserRuleContext {
    public TerminalNode TRUE() { return getToken(EsqlBaseParser.TRUE, 0); }
    public TerminalNode FALSE() { return getToken(EsqlBaseParser.FALSE, 0); }
    @SuppressWarnings("this-escape")
    public BooleanValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_booleanValue; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterBooleanValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitBooleanValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitBooleanValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final BooleanValueContext booleanValue() throws RecognitionException {
    BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
    enterRule(_localctx, 150, RULE_booleanValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(780);
      _la = _input.LA(1);
      if ( !(_la==FALSE || _la==TRUE) ) {
      _errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class NumericValueContext extends ParserRuleContext {
    public DecimalValueContext decimalValue() {
      return getRuleContext(DecimalValueContext.class,0);
    }
    public IntegerValueContext integerValue() {
      return getRuleContext(IntegerValueContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public NumericValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_numericValue; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterNumericValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitNumericValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitNumericValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NumericValueContext numericValue() throws RecognitionException {
    NumericValueContext _localctx = new NumericValueContext(_ctx, getState());
    enterRule(_localctx, 152, RULE_numericValue);
    try {
      setState(784);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,70,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(782);
        decimalValue();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(783);
        integerValue();
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class DecimalValueContext extends ParserRuleContext {
    public TerminalNode DECIMAL_LITERAL() { return getToken(EsqlBaseParser.DECIMAL_LITERAL, 0); }
    public TerminalNode PLUS() { return getToken(EsqlBaseParser.PLUS, 0); }
    public TerminalNode MINUS() { return getToken(EsqlBaseParser.MINUS, 0); }
    @SuppressWarnings("this-escape")
    public DecimalValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_decimalValue; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDecimalValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDecimalValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDecimalValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DecimalValueContext decimalValue() throws RecognitionException {
    DecimalValueContext _localctx = new DecimalValueContext(_ctx, getState());
    enterRule(_localctx, 154, RULE_decimalValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(787);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==PLUS || _la==MINUS) {
        {
        setState(786);
        _la = _input.LA(1);
        if ( !(_la==PLUS || _la==MINUS) ) {
        _errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        }
      }

      setState(789);
      match(DECIMAL_LITERAL);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class IntegerValueContext extends ParserRuleContext {
    public TerminalNode INTEGER_LITERAL() { return getToken(EsqlBaseParser.INTEGER_LITERAL, 0); }
    public TerminalNode PLUS() { return getToken(EsqlBaseParser.PLUS, 0); }
    public TerminalNode MINUS() { return getToken(EsqlBaseParser.MINUS, 0); }
    @SuppressWarnings("this-escape")
    public IntegerValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_integerValue; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIntegerValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIntegerValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIntegerValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IntegerValueContext integerValue() throws RecognitionException {
    IntegerValueContext _localctx = new IntegerValueContext(_ctx, getState());
    enterRule(_localctx, 156, RULE_integerValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(792);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==PLUS || _la==MINUS) {
        {
        setState(791);
        _la = _input.LA(1);
        if ( !(_la==PLUS || _la==MINUS) ) {
        _errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        }
      }

      setState(794);
      match(INTEGER_LITERAL);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class StringContext extends ParserRuleContext {
    public TerminalNode QUOTED_STRING() { return getToken(EsqlBaseParser.QUOTED_STRING, 0); }
    @SuppressWarnings("this-escape")
    public StringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_string; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StringContext string() throws RecognitionException {
    StringContext _localctx = new StringContext(_ctx, getState());
    enterRule(_localctx, 158, RULE_string);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(796);
      match(QUOTED_STRING);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ComparisonOperatorContext extends ParserRuleContext {
    public TerminalNode EQ() { return getToken(EsqlBaseParser.EQ, 0); }
    public TerminalNode NEQ() { return getToken(EsqlBaseParser.NEQ, 0); }
    public TerminalNode LT() { return getToken(EsqlBaseParser.LT, 0); }
    public TerminalNode LTE() { return getToken(EsqlBaseParser.LTE, 0); }
    public TerminalNode GT() { return getToken(EsqlBaseParser.GT, 0); }
    public TerminalNode GTE() { return getToken(EsqlBaseParser.GTE, 0); }
    @SuppressWarnings("this-escape")
    public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_comparisonOperator; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterComparisonOperator(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitComparisonOperator(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitComparisonOperator(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
    ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
    enterRule(_localctx, 160, RULE_comparisonOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(798);
      _la = _input.LA(1);
      if ( !(((((_la - 80)) & ~0x3f) == 0 && ((1L << (_la - 80)) & 125L) != 0)) ) {
      _errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class JoinCommandContext extends ParserRuleContext {
    public Token type;
    public TerminalNode JOIN() { return getToken(EsqlBaseParser.JOIN, 0); }
    public JoinTargetContext joinTarget() {
      return getRuleContext(JoinTargetContext.class,0);
    }
    public JoinConditionContext joinCondition() {
      return getRuleContext(JoinConditionContext.class,0);
    }
    public TerminalNode JOIN_LOOKUP() { return getToken(EsqlBaseParser.JOIN_LOOKUP, 0); }
    public TerminalNode DEV_JOIN_LEFT() { return getToken(EsqlBaseParser.DEV_JOIN_LEFT, 0); }
    public TerminalNode DEV_JOIN_RIGHT() { return getToken(EsqlBaseParser.DEV_JOIN_RIGHT, 0); }
    @SuppressWarnings("this-escape")
    public JoinCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_joinCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterJoinCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitJoinCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitJoinCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final JoinCommandContext joinCommand() throws RecognitionException {
    JoinCommandContext _localctx = new JoinCommandContext(_ctx, getState());
    enterRule(_localctx, 162, RULE_joinCommand);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(800);
      ((JoinCommandContext)_localctx).type = _input.LT(1);
      _la = _input.LA(1);
      if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 109051904L) != 0)) ) {
        ((JoinCommandContext)_localctx).type = (Token)_errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      setState(801);
      match(JOIN);
      setState(802);
      joinTarget();
      setState(803);
      joinCondition();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class JoinTargetContext extends ParserRuleContext {
    public IndexPatternContext index;
    public IndexPatternContext indexPattern() {
      return getRuleContext(IndexPatternContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public JoinTargetContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_joinTarget; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterJoinTarget(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitJoinTarget(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitJoinTarget(this);
      else return visitor.visitChildren(this);
    }
  }

  public final JoinTargetContext joinTarget() throws RecognitionException {
    JoinTargetContext _localctx = new JoinTargetContext(_ctx, getState());
    enterRule(_localctx, 164, RULE_joinTarget);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(805);
      ((JoinTargetContext)_localctx).index = indexPattern();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class JoinConditionContext extends ParserRuleContext {
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
    public List<JoinPredicateContext> joinPredicate() {
      return getRuleContexts(JoinPredicateContext.class);
    }
    public JoinPredicateContext joinPredicate(int i) {
      return getRuleContext(JoinPredicateContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public JoinConditionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_joinCondition; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterJoinCondition(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitJoinCondition(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitJoinCondition(this);
      else return visitor.visitChildren(this);
    }
  }

  public final JoinConditionContext joinCondition() throws RecognitionException {
    JoinConditionContext _localctx = new JoinConditionContext(_ctx, getState());
    enterRule(_localctx, 166, RULE_joinCondition);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(807);
      match(ON);
      setState(808);
      joinPredicate();
      setState(813);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,73,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(809);
          match(COMMA);
          setState(810);
          joinPredicate();
          }
          } 
        }
        setState(815);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,73,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class JoinPredicateContext extends ParserRuleContext {
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public JoinPredicateContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_joinPredicate; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterJoinPredicate(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitJoinPredicate(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitJoinPredicate(this);
      else return visitor.visitChildren(this);
    }
  }

  public final JoinPredicateContext joinPredicate() throws RecognitionException {
    JoinPredicateContext _localctx = new JoinPredicateContext(_ctx, getState());
    enterRule(_localctx, 168, RULE_joinPredicate);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(816);
      valueExpression();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
    switch (ruleIndex) {
    case 1:
      return query_sempred((QueryContext)_localctx, predIndex);
    case 2:
      return sourceCommand_sempred((SourceCommandContext)_localctx, predIndex);
    case 3:
      return processingCommand_sempred((ProcessingCommandContext)_localctx, predIndex);
    case 56:
      return forkSubQueryCommand_sempred((ForkSubQueryCommandContext)_localctx, predIndex);
    case 64:
      return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
    case 68:
      return operatorExpression_sempred((OperatorExpressionContext)_localctx, predIndex);
    case 69:
      return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
    }
    return true;
  }
  private boolean query_sempred(QueryContext _localctx, int predIndex) {
    switch (predIndex) {
    case 0:
      return precpred(_ctx, 1);
    }
    return true;
  }
  private boolean sourceCommand_sempred(SourceCommandContext _localctx, int predIndex) {
    switch (predIndex) {
    case 1:
      return this.isDevVersion();
    case 2:
      return this.isDevVersion();
    }
    return true;
  }
  private boolean processingCommand_sempred(ProcessingCommandContext _localctx, int predIndex) {
    switch (predIndex) {
    case 3:
      return this.isDevVersion();
    case 4:
      return this.isDevVersion();
    case 5:
      return this.isDevVersion();
    case 6:
      return this.isDevVersion();
    case 7:
      return this.isDevVersion();
    }
    return true;
  }
  private boolean forkSubQueryCommand_sempred(ForkSubQueryCommandContext _localctx, int predIndex) {
    switch (predIndex) {
    case 8:
      return precpred(_ctx, 1);
    }
    return true;
  }
  private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 9:
      return precpred(_ctx, 5);
    case 10:
      return precpred(_ctx, 4);
    }
    return true;
  }
  private boolean operatorExpression_sempred(OperatorExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 11:
      return precpred(_ctx, 2);
    case 12:
      return precpred(_ctx, 1);
    }
    return true;
  }
  private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 13:
      return precpred(_ctx, 1);
    }
    return true;
  }

  public static final String _serializedATN =
    "\u0004\u0001\u008b\u0333\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001"+
    "\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004"+
    "\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007"+
    "\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b"+
    "\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007"+
    "\u000f\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007"+
    "\u0012\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007"+
    "\u0015\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002\u0018\u0007"+
    "\u0018\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002\u001b\u0007"+
    "\u001b\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002\u001e\u0007"+
    "\u001e\u0002\u001f\u0007\u001f\u0002 \u0007 \u0002!\u0007!\u0002\"\u0007"+
    "\"\u0002#\u0007#\u0002$\u0007$\u0002%\u0007%\u0002&\u0007&\u0002\'\u0007"+
    "\'\u0002(\u0007(\u0002)\u0007)\u0002*\u0007*\u0002+\u0007+\u0002,\u0007"+
    ",\u0002-\u0007-\u0002.\u0007.\u0002/\u0007/\u00020\u00070\u00021\u0007"+
    "1\u00022\u00072\u00023\u00073\u00024\u00074\u00025\u00075\u00026\u0007"+
    "6\u00027\u00077\u00028\u00078\u00029\u00079\u0002:\u0007:\u0002;\u0007"+
    ";\u0002<\u0007<\u0002=\u0007=\u0002>\u0007>\u0002?\u0007?\u0002@\u0007"+
    "@\u0002A\u0007A\u0002B\u0007B\u0002C\u0007C\u0002D\u0007D\u0002E\u0007"+
    "E\u0002F\u0007F\u0002G\u0007G\u0002H\u0007H\u0002I\u0007I\u0002J\u0007"+
    "J\u0002K\u0007K\u0002L\u0007L\u0002M\u0007M\u0002N\u0007N\u0002O\u0007"+
    "O\u0002P\u0007P\u0002Q\u0007Q\u0002R\u0007R\u0002S\u0007S\u0002T\u0007"+
    "T\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0001\u0001"+
    "\u0001\u0001\u0001\u0001\u0001\u0001\u0005\u0001\u00b4\b\u0001\n\u0001"+
    "\f\u0001\u00b7\t\u0001\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002"+
    "\u0001\u0002\u0001\u0002\u0001\u0002\u0003\u0002\u00c0\b\u0002\u0001\u0003"+
    "\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
    "\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
    "\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
    "\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
    "\u0001\u0003\u0001\u0003\u0003\u0003\u00dd\b\u0003\u0001\u0004\u0001\u0004"+
    "\u0001\u0004\u0001\u0005\u0001\u0005\u0001\u0006\u0001\u0006\u0001\u0006"+
    "\u0001\u0007\u0001\u0007\u0001\u0007\u0005\u0007\u00ea\b\u0007\n\u0007"+
    "\f\u0007\u00ed\t\u0007\u0001\b\u0001\b\u0001\b\u0003\b\u00f2\b\b\u0001"+
    "\b\u0001\b\u0001\t\u0001\t\u0001\t\u0005\t\u00f9\b\t\n\t\f\t\u00fc\t\t"+
    "\u0001\n\u0001\n\u0001\n\u0003\n\u0101\b\n\u0001\u000b\u0001\u000b\u0001"+
    "\u000b\u0001\f\u0001\f\u0001\f\u0001\r\u0001\r\u0001\r\u0005\r\u010c\b"+
    "\r\n\r\f\r\u010f\t\r\u0001\r\u0003\r\u0112\b\r\u0001\u000e\u0001\u000e"+
    "\u0001\u000e\u0001\u000e\u0001\u000e\u0001\u000e\u0001\u000e\u0001\u000e"+
    "\u0001\u000e\u0003\u000e\u011d\b\u000e\u0001\u000f\u0001\u000f\u0001\u0010"+
    "\u0001\u0010\u0001\u0011\u0001\u0011\u0001\u0012\u0001\u0012\u0001\u0013"+
    "\u0001\u0013\u0001\u0013\u0001\u0013\u0005\u0013\u012b\b\u0013\n\u0013"+
    "\f\u0013\u012e\t\u0013\u0001\u0014\u0001\u0014\u0001\u0014\u0001\u0015"+
    "\u0001\u0015\u0003\u0015\u0135\b\u0015\u0001\u0015\u0001\u0015\u0003\u0015"+
    "\u0139\b\u0015\u0001\u0016\u0001\u0016\u0001\u0016\u0005\u0016\u013e\b"+
    "\u0016\n\u0016\f\u0016\u0141\t\u0016\u0001\u0017\u0001\u0017\u0001\u0017"+
    "\u0003\u0017\u0146\b\u0017\u0001\u0018\u0001\u0018\u0001\u0018\u0005\u0018"+
    "\u014b\b\u0018\n\u0018\f\u0018\u014e\t\u0018\u0001\u0019\u0001\u0019\u0001"+
    "\u0019\u0005\u0019\u0153\b\u0019\n\u0019\f\u0019\u0156\t\u0019\u0001\u001a"+
    "\u0001\u001a\u0001\u001a\u0005\u001a\u015b\b\u001a\n\u001a\f\u001a\u015e"+
    "\t\u001a\u0001\u001b\u0001\u001b\u0001\u001c\u0001\u001c\u0001\u001c\u0003"+
    "\u001c\u0165\b\u001c\u0001\u001d\u0001\u001d\u0003\u001d\u0169\b\u001d"+
    "\u0001\u001e\u0001\u001e\u0003\u001e\u016d\b\u001e\u0001\u001f\u0001\u001f"+
    "\u0001\u001f\u0003\u001f\u0172\b\u001f\u0001 \u0001 \u0001 \u0001!\u0001"+
    "!\u0001!\u0001!\u0005!\u017b\b!\n!\f!\u017e\t!\u0001\"\u0001\"\u0003\""+
    "\u0182\b\"\u0001\"\u0001\"\u0003\"\u0186\b\"\u0001#\u0001#\u0001#\u0001"+
    "$\u0001$\u0001$\u0001%\u0001%\u0001%\u0001%\u0005%\u0192\b%\n%\f%\u0195"+
    "\t%\u0001&\u0001&\u0001&\u0001&\u0001&\u0001&\u0001&\u0001&\u0003&\u019f"+
    "\b&\u0001\'\u0001\'\u0001\'\u0001\'\u0003\'\u01a5\b\'\u0001(\u0001(\u0001"+
    "(\u0005(\u01aa\b(\n(\f(\u01ad\t(\u0001)\u0001)\u0001)\u0001)\u0001*\u0001"+
    "*\u0003*\u01b5\b*\u0001+\u0001+\u0001+\u0001+\u0001,\u0001,\u0001,\u0001"+
    "-\u0001-\u0001-\u0001.\u0001.\u0001.\u0001.\u0001/\u0001/\u0001/\u0001"+
    "0\u00010\u00010\u00010\u00030\u01cc\b0\u00010\u00010\u00010\u00010\u0005"+
    "0\u01d2\b0\n0\f0\u01d5\t0\u00030\u01d7\b0\u00011\u00011\u00012\u00012"+
    "\u00012\u00032\u01de\b2\u00012\u00012\u00013\u00013\u00013\u00014\u0001"+
    "4\u00014\u00014\u00034\u01e9\b4\u00014\u00014\u00014\u00014\u00014\u0003"+
    "4\u01f0\b4\u00015\u00015\u00015\u00016\u00046\u01f6\b6\u000b6\f6\u01f7"+
    "\u00017\u00017\u00017\u00017\u00018\u00018\u00018\u00018\u00018\u0001"+
    "8\u00058\u0204\b8\n8\f8\u0207\t8\u00019\u00019\u0001:\u0001:\u0001:\u0001"+
    ":\u0003:\u020f\b:\u0001:\u0001:\u0001:\u0001;\u0001;\u0001;\u0001;\u0001"+
    ";\u0001<\u0001<\u0001<\u0001<\u0003<\u021d\b<\u0001=\u0001=\u0001=\u0001"+
    ">\u0001>\u0001?\u0001?\u0001?\u0001?\u0003?\u0228\b?\u0001?\u0001?\u0001"+
    "?\u0001?\u0001?\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0003"+
    "@\u0236\b@\u0001@\u0001@\u0001@\u0001@\u0001@\u0005@\u023d\b@\n@\f@\u0240"+
    "\t@\u0001@\u0001@\u0001@\u0001@\u0001@\u0003@\u0247\b@\u0001@\u0001@\u0001"+
    "@\u0003@\u024c\b@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0005@\u0254"+
    "\b@\n@\f@\u0257\t@\u0001A\u0001A\u0003A\u025b\bA\u0001A\u0001A\u0001A"+
    "\u0001A\u0001A\u0003A\u0262\bA\u0001A\u0001A\u0001A\u0001A\u0001A\u0003"+
    "A\u0269\bA\u0001A\u0001A\u0001A\u0001A\u0001A\u0005A\u0270\bA\nA\fA\u0273"+
    "\tA\u0001A\u0001A\u0001A\u0001A\u0003A\u0279\bA\u0001A\u0001A\u0001A\u0001"+
    "A\u0001A\u0005A\u0280\bA\nA\fA\u0283\tA\u0001A\u0001A\u0003A\u0287\bA"+
    "\u0001B\u0001B\u0001B\u0003B\u028c\bB\u0001B\u0001B\u0001B\u0001C\u0001"+
    "C\u0001C\u0001C\u0001C\u0003C\u0296\bC\u0001D\u0001D\u0001D\u0001D\u0003"+
    "D\u029c\bD\u0001D\u0001D\u0001D\u0001D\u0001D\u0001D\u0005D\u02a4\bD\n"+
    "D\fD\u02a7\tD\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E"+
    "\u0003E\u02b1\bE\u0001E\u0001E\u0001E\u0005E\u02b6\bE\nE\fE\u02b9\tE\u0001"+
    "F\u0001F\u0001F\u0001F\u0001F\u0001F\u0005F\u02c1\bF\nF\fF\u02c4\tF\u0001"+
    "F\u0001F\u0003F\u02c8\bF\u0003F\u02ca\bF\u0001F\u0001F\u0001G\u0001G\u0001"+
    "H\u0001H\u0001H\u0001H\u0005H\u02d4\bH\nH\fH\u02d7\tH\u0003H\u02d9\bH"+
    "\u0001H\u0001H\u0001I\u0001I\u0001I\u0001I\u0001J\u0001J\u0001J\u0001"+
    "J\u0001J\u0001J\u0001J\u0001J\u0001J\u0001J\u0001J\u0001J\u0001J\u0005"+
    "J\u02ee\bJ\nJ\fJ\u02f1\tJ\u0001J\u0001J\u0001J\u0001J\u0001J\u0001J\u0005"+
    "J\u02f9\bJ\nJ\fJ\u02fc\tJ\u0001J\u0001J\u0001J\u0001J\u0001J\u0001J\u0005"+
    "J\u0304\bJ\nJ\fJ\u0307\tJ\u0001J\u0001J\u0003J\u030b\bJ\u0001K\u0001K"+
    "\u0001L\u0001L\u0003L\u0311\bL\u0001M\u0003M\u0314\bM\u0001M\u0001M\u0001"+
    "N\u0003N\u0319\bN\u0001N\u0001N\u0001O\u0001O\u0001P\u0001P\u0001Q\u0001"+
    "Q\u0001Q\u0001Q\u0001Q\u0001R\u0001R\u0001S\u0001S\u0001S\u0001S\u0005"+
    "S\u032c\bS\nS\fS\u032f\tS\u0001T\u0001T\u0001T\u0000\u0005\u0002p\u0080"+
    "\u0088\u008aU\u0000\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014\u0016"+
    "\u0018\u001a\u001c\u001e \"$&(*,.02468:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprt"+
    "vxz|~\u0080\u0082\u0084\u0086\u0088\u008a\u008c\u008e\u0090\u0092\u0094"+
    "\u0096\u0098\u009a\u009c\u009e\u00a0\u00a2\u00a4\u00a6\u00a8\u0000\n\u0002"+
    "\u000055kk\u0001\u0000ef\u0002\u000099??\u0002\u0000BBEE\u0002\u0000&"+
    "&55\u0001\u0000WX\u0001\u0000Y[\u0002\u0000AANN\u0002\u0000PPRV\u0002"+
    "\u0000\u0017\u0017\u0019\u001a\u0352\u0000\u00aa\u0001\u0000\u0000\u0000"+
    "\u0002\u00ad\u0001\u0000\u0000\u0000\u0004\u00bf\u0001\u0000\u0000\u0000"+
    "\u0006\u00dc\u0001\u0000\u0000\u0000\b\u00de\u0001\u0000\u0000\u0000\n"+
    "\u00e1\u0001\u0000\u0000\u0000\f\u00e3\u0001\u0000\u0000\u0000\u000e\u00e6"+
    "\u0001\u0000\u0000\u0000\u0010\u00f1\u0001\u0000\u0000\u0000\u0012\u00f5"+
    "\u0001\u0000\u0000\u0000\u0014\u00fd\u0001\u0000\u0000\u0000\u0016\u0102"+
    "\u0001\u0000\u0000\u0000\u0018\u0105\u0001\u0000\u0000\u0000\u001a\u0108"+
    "\u0001\u0000\u0000\u0000\u001c\u011c\u0001\u0000\u0000\u0000\u001e\u011e"+
    "\u0001\u0000\u0000\u0000 \u0120\u0001\u0000\u0000\u0000\"\u0122\u0001"+
    "\u0000\u0000\u0000$\u0124\u0001\u0000\u0000\u0000&\u0126\u0001\u0000\u0000"+
    "\u0000(\u012f\u0001\u0000\u0000\u0000*\u0132\u0001\u0000\u0000\u0000,"+
    "\u013a\u0001\u0000\u0000\u0000.\u0142\u0001\u0000\u0000\u00000\u0147\u0001"+
    "\u0000\u0000\u00002\u014f\u0001\u0000\u0000\u00004\u0157\u0001\u0000\u0000"+
    "\u00006\u015f\u0001\u0000\u0000\u00008\u0164\u0001\u0000\u0000\u0000:"+
    "\u0168\u0001\u0000\u0000\u0000<\u016c\u0001\u0000\u0000\u0000>\u0171\u0001"+
    "\u0000\u0000\u0000@\u0173\u0001\u0000\u0000\u0000B\u0176\u0001\u0000\u0000"+
    "\u0000D\u017f\u0001\u0000\u0000\u0000F\u0187\u0001\u0000\u0000\u0000H"+
    "\u018a\u0001\u0000\u0000\u0000J\u018d\u0001\u0000\u0000\u0000L\u019e\u0001"+
    "\u0000\u0000\u0000N\u01a0\u0001\u0000\u0000\u0000P\u01a6\u0001\u0000\u0000"+
    "\u0000R\u01ae\u0001\u0000\u0000\u0000T\u01b4\u0001\u0000\u0000\u0000V"+
    "\u01b6\u0001\u0000\u0000\u0000X\u01ba\u0001\u0000\u0000\u0000Z\u01bd\u0001"+
    "\u0000\u0000\u0000\\\u01c0\u0001\u0000\u0000\u0000^\u01c4\u0001\u0000"+
    "\u0000\u0000`\u01c7\u0001\u0000\u0000\u0000b\u01d8\u0001\u0000\u0000\u0000"+
    "d\u01dd\u0001\u0000\u0000\u0000f\u01e1\u0001\u0000\u0000\u0000h\u01e4"+
    "\u0001\u0000\u0000\u0000j\u01f1\u0001\u0000\u0000\u0000l\u01f5\u0001\u0000"+
    "\u0000\u0000n\u01f9\u0001\u0000\u0000\u0000p\u01fd\u0001\u0000\u0000\u0000"+
    "r\u0208\u0001\u0000\u0000\u0000t\u020a\u0001\u0000\u0000\u0000v\u0213"+
    "\u0001\u0000\u0000\u0000x\u0218\u0001\u0000\u0000\u0000z\u021e\u0001\u0000"+
    "\u0000\u0000|\u0221\u0001\u0000\u0000\u0000~\u0223\u0001\u0000\u0000\u0000"+
    "\u0080\u024b\u0001\u0000\u0000\u0000\u0082\u0286\u0001\u0000\u0000\u0000"+
    "\u0084\u0288\u0001\u0000\u0000\u0000\u0086\u0295\u0001\u0000\u0000\u0000"+
    "\u0088\u029b\u0001\u0000\u0000\u0000\u008a\u02b0\u0001\u0000\u0000\u0000"+
    "\u008c\u02ba\u0001\u0000\u0000\u0000\u008e\u02cd\u0001\u0000\u0000\u0000"+
    "\u0090\u02cf\u0001\u0000\u0000\u0000\u0092\u02dc\u0001\u0000\u0000\u0000"+
    "\u0094\u030a\u0001\u0000\u0000\u0000\u0096\u030c\u0001\u0000\u0000\u0000"+
    "\u0098\u0310\u0001\u0000\u0000\u0000\u009a\u0313\u0001\u0000\u0000\u0000"+
    "\u009c\u0318\u0001\u0000\u0000\u0000\u009e\u031c\u0001\u0000\u0000\u0000"+
    "\u00a0\u031e\u0001\u0000\u0000\u0000\u00a2\u0320\u0001\u0000\u0000\u0000"+
    "\u00a4\u0325\u0001\u0000\u0000\u0000\u00a6\u0327\u0001\u0000\u0000\u0000"+
    "\u00a8\u0330\u0001\u0000\u0000\u0000\u00aa\u00ab\u0003\u0002\u0001\u0000"+
    "\u00ab\u00ac\u0005\u0000\u0000\u0001\u00ac\u0001\u0001\u0000\u0000\u0000"+
    "\u00ad\u00ae\u0006\u0001\uffff\uffff\u0000\u00ae\u00af\u0003\u0004\u0002"+
    "\u0000\u00af\u00b5\u0001\u0000\u0000\u0000\u00b0\u00b1\n\u0001\u0000\u0000"+
    "\u00b1\u00b2\u00054\u0000\u0000\u00b2\u00b4\u0003\u0006\u0003\u0000\u00b3"+
    "\u00b0\u0001\u0000\u0000\u0000\u00b4\u00b7\u0001\u0000\u0000\u0000\u00b5"+
    "\u00b3\u0001\u0000\u0000\u0000\u00b5\u00b6\u0001\u0000\u0000\u0000\u00b6"+
    "\u0003\u0001\u0000\u0000\u0000\u00b7\u00b5\u0001\u0000\u0000\u0000\u00b8"+
    "\u00c0\u0003\u0016\u000b\u0000\u00b9\u00c0\u0003\f\u0006\u0000\u00ba\u00c0"+
    "\u0003^/\u0000\u00bb\u00bc\u0004\u0002\u0001\u0000\u00bc\u00c0\u0003\u0018"+
    "\f\u0000\u00bd\u00be\u0004\u0002\u0002\u0000\u00be\u00c0\u0003Z-\u0000"+
    "\u00bf\u00b8\u0001\u0000\u0000\u0000\u00bf\u00b9\u0001\u0000\u0000\u0000"+
    "\u00bf\u00ba\u0001\u0000\u0000\u0000\u00bf\u00bb\u0001\u0000\u0000\u0000"+
    "\u00bf\u00bd\u0001\u0000\u0000\u0000\u00c0\u0005\u0001\u0000\u0000\u0000"+
    "\u00c1\u00dd\u0003(\u0014\u0000\u00c2\u00dd\u0003\b\u0004\u0000\u00c3"+
    "\u00dd\u0003F#\u0000\u00c4\u00dd\u0003@ \u0000\u00c5\u00dd\u0003*\u0015"+
    "\u0000\u00c6\u00dd\u0003B!\u0000\u00c7\u00dd\u0003H$\u0000\u00c8\u00dd"+
    "\u0003J%\u0000\u00c9\u00dd\u0003N\'\u0000\u00ca\u00dd\u0003V+\u0000\u00cb"+
    "\u00dd\u0003`0\u0000\u00cc\u00dd\u0003X,\u0000\u00cd\u00dd\u0003\u00a2"+
    "Q\u0000\u00ce\u00dd\u0003h4\u0000\u00cf\u00dd\u0003t:\u0000\u00d0\u00dd"+
    "\u0003f3\u0000\u00d1\u00dd\u0003j5\u0000\u00d2\u00d3\u0004\u0003\u0003"+
    "\u0000\u00d3\u00dd\u0003x<\u0000\u00d4\u00d5\u0004\u0003\u0004\u0000\u00d5"+
    "\u00dd\u0003v;\u0000\u00d6\u00d7\u0004\u0003\u0005\u0000\u00d7\u00dd\u0003"+
    "z=\u0000\u00d8\u00d9\u0004\u0003\u0006\u0000\u00d9\u00dd\u0003~?\u0000"+
    "\u00da\u00db\u0004\u0003\u0007\u0000\u00db\u00dd\u0003|>\u0000\u00dc\u00c1"+
    "\u0001\u0000\u0000\u0000\u00dc\u00c2\u0001\u0000\u0000\u0000\u00dc\u00c3"+
    "\u0001\u0000\u0000\u0000\u00dc\u00c4\u0001\u0000\u0000\u0000\u00dc\u00c5"+
    "\u0001\u0000\u0000\u0000\u00dc\u00c6\u0001\u0000\u0000\u0000\u00dc\u00c7"+
    "\u0001\u0000\u0000\u0000\u00dc\u00c8\u0001\u0000\u0000\u0000\u00dc\u00c9"+
    "\u0001\u0000\u0000\u0000\u00dc\u00ca\u0001\u0000\u0000\u0000\u00dc\u00cb"+
    "\u0001\u0000\u0000\u0000\u00dc\u00cc\u0001\u0000\u0000\u0000\u00dc\u00cd"+
    "\u0001\u0000\u0000\u0000\u00dc\u00ce\u0001\u0000\u0000\u0000\u00dc\u00cf"+
    "\u0001\u0000\u0000\u0000\u00dc\u00d0\u0001\u0000\u0000\u0000\u00dc\u00d1"+
    "\u0001\u0000\u0000\u0000\u00dc\u00d2\u0001\u0000\u0000\u0000\u00dc\u00d4"+
    "\u0001\u0000\u0000\u0000\u00dc\u00d6\u0001\u0000\u0000\u0000\u00dc\u00d8"+
    "\u0001\u0000\u0000\u0000\u00dc\u00da\u0001\u0000\u0000\u0000\u00dd\u0007"+
    "\u0001\u0000\u0000\u0000\u00de\u00df\u0005\u0010\u0000\u0000\u00df\u00e0"+
    "\u0003\u0080@\u0000\u00e0\t\u0001\u0000\u0000\u0000\u00e1\u00e2\u0003"+
    "6\u001b\u0000\u00e2\u000b\u0001\u0000\u0000\u0000\u00e3\u00e4\u0005\f"+
    "\u0000\u0000\u00e4\u00e5\u0003\u000e\u0007\u0000\u00e5\r\u0001\u0000\u0000"+
    "\u0000\u00e6\u00eb\u0003\u0010\b\u0000\u00e7\u00e8\u0005>\u0000\u0000"+
    "\u00e8\u00ea\u0003\u0010\b\u0000\u00e9\u00e7\u0001\u0000\u0000\u0000\u00ea"+
    "\u00ed\u0001\u0000\u0000\u0000\u00eb\u00e9\u0001\u0000\u0000\u0000\u00eb"+
    "\u00ec\u0001\u0000\u0000\u0000\u00ec\u000f\u0001\u0000\u0000\u0000\u00ed"+
    "\u00eb\u0001\u0000\u0000\u0000\u00ee\u00ef\u00030\u0018\u0000\u00ef\u00f0"+
    "\u0005:\u0000\u0000\u00f0\u00f2\u0001\u0000\u0000\u0000\u00f1\u00ee\u0001"+
    "\u0000\u0000\u0000\u00f1\u00f2\u0001\u0000\u0000\u0000\u00f2\u00f3\u0001"+
    "\u0000\u0000\u0000\u00f3\u00f4\u0003\u0080@\u0000\u00f4\u0011\u0001\u0000"+
    "\u0000\u0000\u00f5\u00fa\u0003\u0014\n\u0000\u00f6\u00f7\u0005>\u0000"+
    "\u0000\u00f7\u00f9\u0003\u0014\n\u0000\u00f8\u00f6\u0001\u0000\u0000\u0000"+
    "\u00f9\u00fc\u0001\u0000\u0000\u0000\u00fa\u00f8\u0001\u0000\u0000\u0000"+
    "\u00fa\u00fb\u0001\u0000\u0000\u0000\u00fb\u0013\u0001\u0000\u0000\u0000"+
    "\u00fc\u00fa\u0001\u0000\u0000\u0000\u00fd\u0100\u00030\u0018\u0000\u00fe"+
    "\u00ff\u0005:\u0000\u0000\u00ff\u0101\u0003\u0080@\u0000\u0100\u00fe\u0001"+
    "\u0000\u0000\u0000\u0100\u0101\u0001\u0000\u0000\u0000\u0101\u0015\u0001"+
    "\u0000\u0000\u0000\u0102\u0103\u0005\u0013\u0000\u0000\u0103\u0104\u0003"+
    "\u001a\r\u0000\u0104\u0017\u0001\u0000\u0000\u0000\u0105\u0106\u0005\u0014"+
    "\u0000\u0000\u0106\u0107\u0003\u001a\r\u0000\u0107\u0019\u0001\u0000\u0000"+
    "\u0000\u0108\u010d\u0003\u001c\u000e\u0000\u0109\u010a\u0005>\u0000\u0000"+
    "\u010a\u010c\u0003\u001c\u000e\u0000\u010b\u0109\u0001\u0000\u0000\u0000"+
    "\u010c\u010f\u0001\u0000\u0000\u0000\u010d\u010b\u0001\u0000\u0000\u0000"+
    "\u010d\u010e\u0001\u0000\u0000\u0000\u010e\u0111\u0001\u0000\u0000\u0000"+
    "\u010f\u010d\u0001\u0000\u0000\u0000\u0110\u0112\u0003&\u0013\u0000\u0111"+
    "\u0110\u0001\u0000\u0000\u0000\u0111\u0112\u0001\u0000\u0000\u0000\u0112"+
    "\u001b\u0001\u0000\u0000\u0000\u0113\u0114\u0003\u001e\u000f\u0000\u0114"+
    "\u0115\u0005=\u0000\u0000\u0115\u0116\u0003\"\u0011\u0000\u0116\u011d"+
    "\u0001\u0000\u0000\u0000\u0117\u0118\u0003\"\u0011\u0000\u0118\u0119\u0005"+
    "<\u0000\u0000\u0119\u011a\u0003 \u0010\u0000\u011a\u011d\u0001\u0000\u0000"+
    "\u0000\u011b\u011d\u0003$\u0012\u0000\u011c\u0113\u0001\u0000\u0000\u0000"+
    "\u011c\u0117\u0001\u0000\u0000\u0000\u011c\u011b\u0001\u0000\u0000\u0000"+
    "\u011d\u001d\u0001\u0000\u0000\u0000\u011e\u011f\u0005k\u0000\u0000\u011f"+
    "\u001f\u0001\u0000\u0000\u0000\u0120\u0121\u0005k\u0000\u0000\u0121!\u0001"+
    "\u0000\u0000\u0000\u0122\u0123\u0005k\u0000\u0000\u0123#\u0001\u0000\u0000"+
    "\u0000\u0124\u0125\u0007\u0000\u0000\u0000\u0125%\u0001\u0000\u0000\u0000"+
    "\u0126\u0127\u0005j\u0000\u0000\u0127\u012c\u0005k\u0000\u0000\u0128\u0129"+
    "\u0005>\u0000\u0000\u0129\u012b\u0005k\u0000\u0000\u012a\u0128\u0001\u0000"+
    "\u0000\u0000\u012b\u012e\u0001\u0000\u0000\u0000\u012c\u012a\u0001\u0000"+
    "\u0000\u0000\u012c\u012d\u0001\u0000\u0000\u0000\u012d\'\u0001\u0000\u0000"+
    "\u0000\u012e\u012c\u0001\u0000\u0000\u0000\u012f\u0130\u0005\t\u0000\u0000"+
    "\u0130\u0131\u0003\u000e\u0007\u0000\u0131)\u0001\u0000\u0000\u0000\u0132"+
    "\u0134\u0005\u000f\u0000\u0000\u0133\u0135\u0003,\u0016\u0000\u0134\u0133"+
    "\u0001\u0000\u0000\u0000\u0134\u0135\u0001\u0000\u0000\u0000\u0135\u0138"+
    "\u0001\u0000\u0000\u0000\u0136\u0137\u0005;\u0000\u0000\u0137\u0139\u0003"+
    "\u000e\u0007\u0000\u0138\u0136\u0001\u0000\u0000\u0000\u0138\u0139\u0001"+
    "\u0000\u0000\u0000\u0139+\u0001\u0000\u0000\u0000\u013a\u013f\u0003.\u0017"+
    "\u0000\u013b\u013c\u0005>\u0000\u0000\u013c\u013e\u0003.\u0017\u0000\u013d"+
    "\u013b\u0001\u0000\u0000\u0000\u013e\u0141\u0001\u0000\u0000\u0000\u013f"+
    "\u013d\u0001\u0000\u0000\u0000\u013f\u0140\u0001\u0000\u0000\u0000\u0140"+
    "-\u0001\u0000\u0000\u0000\u0141\u013f\u0001\u0000\u0000\u0000\u0142\u0145"+
    "\u0003\u0010\b\u0000\u0143\u0144\u0005\u0010\u0000\u0000\u0144\u0146\u0003"+
    "\u0080@\u0000\u0145\u0143\u0001\u0000\u0000\u0000\u0145\u0146\u0001\u0000"+
    "\u0000\u0000\u0146/\u0001\u0000\u0000\u0000\u0147\u014c\u0003>\u001f\u0000"+
    "\u0148\u0149\u0005@\u0000\u0000\u0149\u014b\u0003>\u001f\u0000\u014a\u0148"+
    "\u0001\u0000\u0000\u0000\u014b\u014e\u0001\u0000\u0000\u0000\u014c\u014a"+
    "\u0001\u0000\u0000\u0000\u014c\u014d\u0001\u0000\u0000\u0000\u014d1\u0001"+
    "\u0000\u0000\u0000\u014e\u014c\u0001\u0000\u0000\u0000\u014f\u0154\u0003"+
    "8\u001c\u0000\u0150\u0151\u0005@\u0000\u0000\u0151\u0153\u00038\u001c"+
    "\u0000\u0152\u0150\u0001\u0000\u0000\u0000\u0153\u0156\u0001\u0000\u0000"+
    "\u0000\u0154\u0152\u0001\u0000\u0000\u0000\u0154\u0155\u0001\u0000\u0000"+
    "\u0000\u01553\u0001\u0000\u0000\u0000\u0156\u0154\u0001\u0000\u0000\u0000"+
    "\u0157\u015c\u00032\u0019\u0000\u0158\u0159\u0005>\u0000\u0000\u0159\u015b"+
    "\u00032\u0019\u0000\u015a\u0158\u0001\u0000\u0000\u0000\u015b\u015e\u0001"+
    "\u0000\u0000\u0000\u015c\u015a\u0001\u0000\u0000\u0000\u015c\u015d\u0001"+
    "\u0000\u0000\u0000\u015d5\u0001\u0000\u0000\u0000\u015e\u015c\u0001\u0000"+
    "\u0000\u0000\u015f\u0160\u0007\u0001\u0000\u0000\u01607\u0001\u0000\u0000"+
    "\u0000\u0161\u0165\u0005\u0080\u0000\u0000\u0162\u0165\u0003:\u001d\u0000"+
    "\u0163\u0165\u0003<\u001e\u0000\u0164\u0161\u0001\u0000\u0000\u0000\u0164"+
    "\u0162\u0001\u0000\u0000\u0000\u0164\u0163\u0001\u0000\u0000\u0000\u0165"+
    "9\u0001\u0000\u0000\u0000\u0166\u0169\u0005L\u0000\u0000\u0167\u0169\u0005"+
    "_\u0000\u0000\u0168\u0166\u0001\u0000\u0000\u0000\u0168\u0167\u0001\u0000"+
    "\u0000\u0000\u0169;\u0001\u0000\u0000\u0000\u016a\u016d\u0005^\u0000\u0000"+
    "\u016b\u016d\u0005`\u0000\u0000\u016c\u016a\u0001\u0000\u0000\u0000\u016c"+
    "\u016b\u0001\u0000\u0000\u0000\u016d=\u0001\u0000\u0000\u0000\u016e\u0172"+
    "\u00036\u001b\u0000\u016f\u0172\u0003:\u001d\u0000\u0170\u0172\u0003<"+
    "\u001e\u0000\u0171\u016e\u0001\u0000\u0000\u0000\u0171\u016f\u0001\u0000"+
    "\u0000\u0000\u0171\u0170\u0001\u0000\u0000\u0000\u0172?\u0001\u0000\u0000"+
    "\u0000\u0173\u0174\u0005\u000b\u0000\u0000\u0174\u0175\u0003\u0094J\u0000"+
    "\u0175A\u0001\u0000\u0000\u0000\u0176\u0177\u0005\u000e\u0000\u0000\u0177"+
    "\u017c\u0003D\"\u0000\u0178\u0179\u0005>\u0000\u0000\u0179\u017b\u0003"+
    "D\"\u0000\u017a\u0178\u0001\u0000\u0000\u0000\u017b\u017e\u0001\u0000"+
    "\u0000\u0000\u017c\u017a\u0001\u0000\u0000\u0000\u017c\u017d\u0001\u0000"+
    "\u0000\u0000\u017dC\u0001\u0000\u0000\u0000\u017e\u017c\u0001\u0000\u0000"+
    "\u0000\u017f\u0181\u0003\u0080@\u0000\u0180\u0182\u0007\u0002\u0000\u0000"+
    "\u0181\u0180\u0001\u0000\u0000\u0000\u0181\u0182\u0001\u0000\u0000\u0000"+
    "\u0182\u0185\u0001\u0000\u0000\u0000\u0183\u0184\u0005I\u0000\u0000\u0184"+
    "\u0186\u0007\u0003\u0000\u0000\u0185\u0183\u0001\u0000\u0000\u0000\u0185"+
    "\u0186\u0001\u0000\u0000\u0000\u0186E\u0001\u0000\u0000\u0000\u0187\u0188"+
    "\u0005\u001e\u0000\u0000\u0188\u0189\u00034\u001a\u0000\u0189G\u0001\u0000"+
    "\u0000\u0000\u018a\u018b\u0005\u001d\u0000\u0000\u018b\u018c\u00034\u001a"+
    "\u0000\u018cI\u0001\u0000\u0000\u0000\u018d\u018e\u0005 \u0000\u0000\u018e"+
    "\u0193\u0003L&\u0000\u018f\u0190\u0005>\u0000\u0000\u0190\u0192\u0003"+
    "L&\u0000\u0191\u018f\u0001\u0000\u0000\u0000\u0192\u0195\u0001\u0000\u0000"+
    "\u0000\u0193\u0191\u0001\u0000\u0000\u0000\u0193\u0194\u0001\u0000\u0000"+
    "\u0000\u0194K\u0001\u0000\u0000\u0000\u0195\u0193\u0001\u0000\u0000\u0000"+
    "\u0196\u0197\u00032\u0019\u0000\u0197\u0198\u0005\u0084\u0000\u0000\u0198"+
    "\u0199\u00032\u0019\u0000\u0199\u019f\u0001\u0000\u0000\u0000\u019a\u019b"+
    "\u00032\u0019\u0000\u019b\u019c\u0005:\u0000\u0000\u019c\u019d\u00032"+
    "\u0019\u0000\u019d\u019f\u0001\u0000\u0000\u0000\u019e\u0196\u0001\u0000"+
    "\u0000\u0000\u019e\u019a\u0001\u0000\u0000\u0000\u019fM\u0001\u0000\u0000"+
    "\u0000\u01a0\u01a1\u0005\b\u0000\u0000\u01a1\u01a2\u0003\u008aE\u0000"+
    "\u01a2\u01a4\u0003\u009eO\u0000\u01a3\u01a5\u0003P(\u0000\u01a4\u01a3"+
    "\u0001\u0000\u0000\u0000\u01a4\u01a5\u0001\u0000\u0000\u0000\u01a5O\u0001"+
    "\u0000\u0000\u0000\u01a6\u01ab\u0003R)\u0000\u01a7\u01a8\u0005>\u0000"+
    "\u0000\u01a8\u01aa\u0003R)\u0000\u01a9\u01a7\u0001\u0000\u0000\u0000\u01aa"+
    "\u01ad\u0001\u0000\u0000\u0000\u01ab\u01a9\u0001\u0000\u0000\u0000\u01ab"+
    "\u01ac\u0001\u0000\u0000\u0000\u01acQ\u0001\u0000\u0000\u0000\u01ad\u01ab"+
    "\u0001\u0000\u0000\u0000\u01ae\u01af\u00036\u001b\u0000\u01af\u01b0\u0005"+
    ":\u0000\u0000\u01b0\u01b1\u0003\u0094J\u0000\u01b1S\u0001\u0000\u0000"+
    "\u0000\u01b2\u01b3\u0005O\u0000\u0000\u01b3\u01b5\u0003\u0090H\u0000\u01b4"+
    "\u01b2\u0001\u0000\u0000\u0000\u01b4\u01b5\u0001\u0000\u0000\u0000\u01b5"+
    "U\u0001\u0000\u0000\u0000\u01b6\u01b7\u0005\n\u0000\u0000\u01b7\u01b8"+
    "\u0003\u008aE\u0000\u01b8\u01b9\u0003\u009eO\u0000\u01b9W\u0001\u0000"+
    "\u0000\u0000\u01ba\u01bb\u0005\u001c\u0000\u0000\u01bb\u01bc\u00030\u0018"+
    "\u0000\u01bcY\u0001\u0000\u0000\u0000\u01bd\u01be\u0005\u0006\u0000\u0000"+
    "\u01be\u01bf\u0003\\.\u0000\u01bf[\u0001\u0000\u0000\u0000\u01c0\u01c1"+
    "\u0005c\u0000\u0000\u01c1\u01c2\u0003\u0002\u0001\u0000\u01c2\u01c3\u0005"+
    "d\u0000\u0000\u01c3]\u0001\u0000\u0000\u0000\u01c4\u01c5\u0005!\u0000"+
    "\u0000\u01c5\u01c6\u0005\u0088\u0000\u0000\u01c6_\u0001\u0000\u0000\u0000"+
    "\u01c7\u01c8\u0005\u0005\u0000\u0000\u01c8\u01cb\u0003b1\u0000\u01c9\u01ca"+
    "\u0005J\u0000\u0000\u01ca\u01cc\u00032\u0019\u0000\u01cb\u01c9\u0001\u0000"+
    "\u0000\u0000\u01cb\u01cc\u0001\u0000\u0000\u0000\u01cc\u01d6\u0001\u0000"+
    "\u0000\u0000\u01cd\u01ce\u0005O\u0000\u0000\u01ce\u01d3\u0003d2\u0000"+
    "\u01cf\u01d0\u0005>\u0000\u0000\u01d0\u01d2\u0003d2\u0000\u01d1\u01cf"+
    "\u0001\u0000\u0000\u0000\u01d2\u01d5\u0001\u0000\u0000\u0000\u01d3\u01d1"+
    "\u0001\u0000\u0000\u0000\u01d3\u01d4\u0001\u0000\u0000\u0000\u01d4\u01d7"+
    "\u0001\u0000\u0000\u0000\u01d5\u01d3\u0001\u0000\u0000\u0000\u01d6\u01cd"+
    "\u0001\u0000\u0000\u0000\u01d6\u01d7\u0001\u0000\u0000\u0000\u01d7a\u0001"+
    "\u0000\u0000\u0000\u01d8\u01d9\u0007\u0004\u0000\u0000\u01d9c\u0001\u0000"+
    "\u0000\u0000\u01da\u01db\u00032\u0019\u0000\u01db\u01dc\u0005:\u0000\u0000"+
    "\u01dc\u01de\u0001\u0000\u0000\u0000\u01dd\u01da\u0001\u0000\u0000\u0000"+
    "\u01dd\u01de\u0001\u0000\u0000\u0000\u01de\u01df\u0001\u0000\u0000\u0000"+
    "\u01df\u01e0\u00032\u0019\u0000\u01e0e\u0001\u0000\u0000\u0000\u01e1\u01e2"+
    "\u0005\r\u0000\u0000\u01e2\u01e3\u0003\u0094J\u0000\u01e3g\u0001\u0000"+
    "\u0000\u0000\u01e4\u01e5\u0005\u0004\u0000\u0000\u01e5\u01e8\u00030\u0018"+
    "\u0000\u01e6\u01e7\u0005J\u0000\u0000\u01e7\u01e9\u00030\u0018\u0000\u01e8"+
    "\u01e6\u0001\u0000\u0000\u0000\u01e8\u01e9\u0001\u0000\u0000\u0000\u01e9"+
    "\u01ef\u0001\u0000\u0000\u0000\u01ea\u01eb\u0005\u0084\u0000\u0000\u01eb"+
    "\u01ec\u00030\u0018\u0000\u01ec\u01ed\u0005>\u0000\u0000\u01ed\u01ee\u0003"+
    "0\u0018\u0000\u01ee\u01f0\u0001\u0000\u0000\u0000\u01ef\u01ea\u0001\u0000"+
    "\u0000\u0000\u01ef\u01f0\u0001\u0000\u0000\u0000\u01f0i\u0001\u0000\u0000"+
    "\u0000\u01f1\u01f2\u0005\u0015\u0000\u0000\u01f2\u01f3\u0003l6\u0000\u01f3"+
    "k\u0001\u0000\u0000\u0000\u01f4\u01f6\u0003n7\u0000\u01f5\u01f4\u0001"+
    "\u0000\u0000\u0000\u01f6\u01f7\u0001\u0000\u0000\u0000\u01f7\u01f5\u0001"+
    "\u0000\u0000\u0000\u01f7\u01f8\u0001\u0000\u0000\u0000\u01f8m\u0001\u0000"+
    "\u0000\u0000\u01f9\u01fa\u0005c\u0000\u0000\u01fa\u01fb\u0003p8\u0000"+
    "\u01fb\u01fc\u0005d\u0000\u0000\u01fco\u0001\u0000\u0000\u0000\u01fd\u01fe"+
    "\u00068\uffff\uffff\u0000\u01fe\u01ff\u0003r9\u0000\u01ff\u0205\u0001"+
    "\u0000\u0000\u0000\u0200\u0201\n\u0001\u0000\u0000\u0201\u0202\u00054"+
    "\u0000\u0000\u0202\u0204\u0003r9\u0000\u0203\u0200\u0001\u0000\u0000\u0000"+
    "\u0204\u0207\u0001\u0000\u0000\u0000\u0205\u0203\u0001\u0000\u0000\u0000"+
    "\u0205\u0206\u0001\u0000\u0000\u0000\u0206q\u0001\u0000\u0000\u0000\u0207"+
    "\u0205\u0001\u0000\u0000\u0000\u0208\u0209\u0003\u0006\u0003\u0000\u0209"+
    "s\u0001\u0000\u0000\u0000\u020a\u020e\u0005\u0007\u0000\u0000\u020b\u020c"+
    "\u00030\u0018\u0000\u020c\u020d\u0005:\u0000\u0000\u020d\u020f\u0001\u0000"+
    "\u0000\u0000\u020e\u020b\u0001\u0000\u0000\u0000\u020e\u020f\u0001\u0000"+
    "\u0000\u0000\u020f\u0210\u0001\u0000\u0000\u0000\u0210\u0211\u0003\u008a"+
    "E\u0000\u0211\u0212\u0003T*\u0000\u0212u\u0001\u0000\u0000\u0000\u0213"+
    "\u0214\u0005\u001b\u0000\u0000\u0214\u0215\u0003\u001c\u000e\u0000\u0215"+
    "\u0216\u0005J\u0000\u0000\u0216\u0217\u00034\u001a\u0000\u0217w\u0001"+
    "\u0000\u0000\u0000\u0218\u0219\u0005\u0011\u0000\u0000\u0219\u021c\u0003"+
    ",\u0016\u0000\u021a\u021b\u0005;\u0000\u0000\u021b\u021d\u0003\u000e\u0007"+
    "\u0000\u021c\u021a\u0001\u0000\u0000\u0000\u021c\u021d\u0001\u0000\u0000"+
    "\u0000\u021dy\u0001\u0000\u0000\u0000\u021e\u021f\u0005\u001f\u0000\u0000"+
    "\u021f\u0220\u00034\u001a\u0000\u0220{\u0001\u0000\u0000\u0000\u0221\u0222"+
    "\u0005\u0016\u0000\u0000\u0222}\u0001\u0000\u0000\u0000\u0223\u0227\u0005"+
    "\u0012\u0000\u0000\u0224\u0225\u00030\u0018\u0000\u0225\u0226\u0005:\u0000"+
    "\u0000\u0226\u0228\u0001\u0000\u0000\u0000\u0227\u0224\u0001\u0000\u0000"+
    "\u0000\u0227\u0228\u0001\u0000\u0000\u0000\u0228\u0229\u0001\u0000\u0000"+
    "\u0000\u0229\u022a\u0003\u0094J\u0000\u022a\u022b\u0005J\u0000\u0000\u022b"+
    "\u022c\u0003\u0012\t\u0000\u022c\u022d\u0003T*\u0000\u022d\u007f\u0001"+
    "\u0000\u0000\u0000\u022e\u022f\u0006@\uffff\uffff\u0000\u022f\u0230\u0005"+
    "G\u0000\u0000\u0230\u024c\u0003\u0080@\b\u0231\u024c\u0003\u0086C\u0000"+
    "\u0232\u024c\u0003\u0082A\u0000\u0233\u0235\u0003\u0086C\u0000\u0234\u0236"+
    "\u0005G\u0000\u0000\u0235\u0234\u0001\u0000\u0000\u0000\u0235\u0236\u0001"+
    "\u0000\u0000\u0000\u0236\u0237\u0001\u0000\u0000\u0000\u0237\u0238\u0005"+
    "C\u0000\u0000\u0238\u0239\u0005c\u0000\u0000\u0239\u023e\u0003\u0086C"+
    "\u0000\u023a\u023b\u0005>\u0000\u0000\u023b\u023d\u0003\u0086C\u0000\u023c"+
    "\u023a\u0001\u0000\u0000\u0000\u023d\u0240\u0001\u0000\u0000\u0000\u023e"+
    "\u023c\u0001\u0000\u0000\u0000\u023e\u023f\u0001\u0000\u0000\u0000\u023f"+
    "\u0241\u0001\u0000\u0000\u0000\u0240\u023e\u0001\u0000\u0000\u0000\u0241"+
    "\u0242\u0005d\u0000\u0000\u0242\u024c\u0001\u0000\u0000\u0000\u0243\u0244"+
    "\u0003\u0086C\u0000\u0244\u0246\u0005D\u0000\u0000\u0245\u0247\u0005G"+
    "\u0000\u0000\u0246\u0245\u0001\u0000\u0000\u0000\u0246\u0247\u0001\u0000"+
    "\u0000\u0000\u0247\u0248\u0001\u0000\u0000\u0000\u0248\u0249\u0005H\u0000"+
    "\u0000\u0249\u024c\u0001\u0000\u0000\u0000\u024a\u024c\u0003\u0084B\u0000"+
    "\u024b\u022e\u0001\u0000\u0000\u0000\u024b\u0231\u0001\u0000\u0000\u0000"+
    "\u024b\u0232\u0001\u0000\u0000\u0000\u024b\u0233\u0001\u0000\u0000\u0000"+
    "\u024b\u0243\u0001\u0000\u0000\u0000\u024b\u024a\u0001\u0000\u0000\u0000"+
    "\u024c\u0255\u0001\u0000\u0000\u0000\u024d\u024e\n\u0005\u0000\u0000\u024e"+
    "\u024f\u00058\u0000\u0000\u024f\u0254\u0003\u0080@\u0006\u0250\u0251\n"+
    "\u0004\u0000\u0000\u0251\u0252\u0005K\u0000\u0000\u0252\u0254\u0003\u0080"+
    "@\u0005\u0253\u024d\u0001\u0000\u0000\u0000\u0253\u0250\u0001\u0000\u0000"+
    "\u0000\u0254\u0257\u0001\u0000\u0000\u0000\u0255\u0253\u0001\u0000\u0000"+
    "\u0000\u0255\u0256\u0001\u0000\u0000\u0000\u0256\u0081\u0001\u0000\u0000"+
    "\u0000\u0257\u0255\u0001\u0000\u0000\u0000\u0258\u025a\u0003\u0086C\u0000"+
    "\u0259\u025b\u0005G\u0000\u0000\u025a\u0259\u0001\u0000\u0000\u0000\u025a"+
    "\u025b\u0001\u0000\u0000\u0000\u025b\u025c\u0001\u0000\u0000\u0000\u025c"+
    "\u025d\u0005F\u0000\u0000\u025d\u025e\u0003\u009eO\u0000\u025e\u0287\u0001"+
    "\u0000\u0000\u0000\u025f\u0261\u0003\u0086C\u0000\u0260\u0262\u0005G\u0000"+
    "\u0000\u0261\u0260\u0001\u0000\u0000\u0000\u0261\u0262\u0001\u0000\u0000"+
    "\u0000\u0262\u0263\u0001\u0000\u0000\u0000\u0263\u0264\u0005M\u0000\u0000"+
    "\u0264\u0265\u0003\u009eO\u0000\u0265\u0287\u0001\u0000\u0000\u0000\u0266"+
    "\u0268\u0003\u0086C\u0000\u0267\u0269\u0005G\u0000\u0000\u0268\u0267\u0001"+
    "\u0000\u0000\u0000\u0268\u0269\u0001\u0000\u0000\u0000\u0269\u026a\u0001"+
    "\u0000\u0000\u0000\u026a\u026b\u0005F\u0000\u0000\u026b\u026c\u0005c\u0000"+
    "\u0000\u026c\u0271\u0003\u009eO\u0000\u026d\u026e\u0005>\u0000\u0000\u026e"+
    "\u0270\u0003\u009eO\u0000\u026f\u026d\u0001\u0000\u0000\u0000\u0270\u0273"+
    "\u0001\u0000\u0000\u0000\u0271\u026f\u0001\u0000\u0000\u0000\u0271\u0272"+
    "\u0001\u0000\u0000\u0000\u0272\u0274\u0001\u0000\u0000\u0000\u0273\u0271"+
    "\u0001\u0000\u0000\u0000\u0274\u0275\u0005d\u0000\u0000\u0275\u0287\u0001"+
    "\u0000\u0000\u0000\u0276\u0278\u0003\u0086C\u0000\u0277\u0279\u0005G\u0000"+
    "\u0000\u0278\u0277\u0001\u0000\u0000\u0000\u0278\u0279\u0001\u0000\u0000"+
    "\u0000\u0279\u027a\u0001\u0000\u0000\u0000\u027a\u027b\u0005M\u0000\u0000"+
    "\u027b\u027c\u0005c\u0000\u0000\u027c\u0281\u0003\u009eO\u0000\u027d\u027e"+
    "\u0005>\u0000\u0000\u027e\u0280\u0003\u009eO\u0000\u027f\u027d\u0001\u0000"+
    "\u0000\u0000\u0280\u0283\u0001\u0000\u0000\u0000\u0281\u027f\u0001\u0000"+
    "\u0000\u0000\u0281\u0282\u0001\u0000\u0000\u0000\u0282\u0284\u0001\u0000"+
    "\u0000\u0000\u0283\u0281\u0001\u0000\u0000\u0000\u0284\u0285\u0005d\u0000"+
    "\u0000\u0285\u0287\u0001\u0000\u0000\u0000\u0286\u0258\u0001\u0000\u0000"+
    "\u0000\u0286\u025f\u0001\u0000\u0000\u0000\u0286\u0266\u0001\u0000\u0000"+
    "\u0000\u0286\u0276\u0001\u0000\u0000\u0000\u0287\u0083\u0001\u0000\u0000"+
    "\u0000\u0288\u028b\u00030\u0018\u0000\u0289\u028a\u0005<\u0000\u0000\u028a"+
    "\u028c\u0003\n\u0005\u0000\u028b\u0289\u0001\u0000\u0000\u0000\u028b\u028c"+
    "\u0001\u0000\u0000\u0000\u028c\u028d\u0001\u0000\u0000\u0000\u028d\u028e"+
    "\u0005=\u0000\u0000\u028e\u028f\u0003\u0094J\u0000\u028f\u0085\u0001\u0000"+
    "\u0000\u0000\u0290\u0296\u0003\u0088D\u0000\u0291\u0292\u0003\u0088D\u0000"+
    "\u0292\u0293\u0003\u00a0P\u0000\u0293\u0294\u0003\u0088D\u0000\u0294\u0296"+
    "\u0001\u0000\u0000\u0000\u0295\u0290\u0001\u0000\u0000\u0000\u0295\u0291"+
    "\u0001\u0000\u0000\u0000\u0296\u0087\u0001\u0000\u0000\u0000\u0297\u0298"+
    "\u0006D\uffff\uffff\u0000\u0298\u029c\u0003\u008aE\u0000\u0299\u029a\u0007"+
    "\u0005\u0000\u0000\u029a\u029c\u0003\u0088D\u0003\u029b\u0297\u0001\u0000"+
    "\u0000\u0000\u029b\u0299\u0001\u0000\u0000\u0000\u029c\u02a5\u0001\u0000"+
    "\u0000\u0000\u029d\u029e\n\u0002\u0000\u0000\u029e\u029f\u0007\u0006\u0000"+
    "\u0000\u029f\u02a4\u0003\u0088D\u0003\u02a0\u02a1\n\u0001\u0000\u0000"+
    "\u02a1\u02a2\u0007\u0005\u0000\u0000\u02a2\u02a4\u0003\u0088D\u0002\u02a3"+
    "\u029d\u0001\u0000\u0000\u0000\u02a3\u02a0\u0001\u0000\u0000\u0000\u02a4"+
    "\u02a7\u0001\u0000\u0000\u0000\u02a5\u02a3\u0001\u0000\u0000\u0000\u02a5"+
    "\u02a6\u0001\u0000\u0000\u0000\u02a6\u0089\u0001\u0000\u0000\u0000\u02a7"+
    "\u02a5\u0001\u0000\u0000\u0000\u02a8\u02a9\u0006E\uffff\uffff\u0000\u02a9"+
    "\u02b1\u0003\u0094J\u0000\u02aa\u02b1\u00030\u0018\u0000\u02ab\u02b1\u0003"+
    "\u008cF\u0000\u02ac\u02ad\u0005c\u0000\u0000\u02ad\u02ae\u0003\u0080@"+
    "\u0000\u02ae\u02af\u0005d\u0000\u0000\u02af\u02b1\u0001\u0000\u0000\u0000"+
    "\u02b0\u02a8\u0001\u0000\u0000\u0000\u02b0\u02aa\u0001\u0000\u0000\u0000"+
    "\u02b0\u02ab\u0001\u0000\u0000\u0000\u02b0\u02ac\u0001\u0000\u0000\u0000"+
    "\u02b1\u02b7\u0001\u0000\u0000\u0000\u02b2\u02b3\n\u0001\u0000\u0000\u02b3"+
    "\u02b4\u0005<\u0000\u0000\u02b4\u02b6\u0003\n\u0005\u0000\u02b5\u02b2"+
    "\u0001\u0000\u0000\u0000\u02b6\u02b9\u0001\u0000\u0000\u0000\u02b7\u02b5"+
    "\u0001\u0000\u0000\u0000\u02b7\u02b8\u0001\u0000\u0000\u0000\u02b8\u008b"+
    "\u0001\u0000\u0000\u0000\u02b9\u02b7\u0001\u0000\u0000\u0000\u02ba\u02bb"+
    "\u0003\u008eG\u0000\u02bb\u02c9\u0005c\u0000\u0000\u02bc\u02ca\u0005Y"+
    "\u0000\u0000\u02bd\u02c2\u0003\u0080@\u0000\u02be\u02bf\u0005>\u0000\u0000"+
    "\u02bf\u02c1\u0003\u0080@\u0000\u02c0\u02be\u0001\u0000\u0000\u0000\u02c1"+
    "\u02c4\u0001\u0000\u0000\u0000\u02c2\u02c0\u0001\u0000\u0000\u0000\u02c2"+
    "\u02c3\u0001\u0000\u0000\u0000\u02c3\u02c7\u0001\u0000\u0000\u0000\u02c4"+
    "\u02c2\u0001\u0000\u0000\u0000\u02c5\u02c6\u0005>\u0000\u0000\u02c6\u02c8"+
    "\u0003\u0090H\u0000\u02c7\u02c5\u0001\u0000\u0000\u0000\u02c7\u02c8\u0001"+
    "\u0000\u0000\u0000\u02c8\u02ca\u0001\u0000\u0000\u0000\u02c9\u02bc\u0001"+
    "\u0000\u0000\u0000\u02c9\u02bd\u0001\u0000\u0000\u0000\u02c9\u02ca\u0001"+
    "\u0000\u0000\u0000\u02ca\u02cb\u0001\u0000\u0000\u0000\u02cb\u02cc\u0005"+
    "d\u0000\u0000\u02cc\u008d\u0001\u0000\u0000\u0000\u02cd\u02ce\u0003>\u001f"+
    "\u0000\u02ce\u008f\u0001\u0000\u0000\u0000\u02cf\u02d8\u0005\\\u0000\u0000"+
    "\u02d0\u02d5\u0003\u0092I\u0000\u02d1\u02d2\u0005>\u0000\u0000\u02d2\u02d4"+
    "\u0003\u0092I\u0000\u02d3\u02d1\u0001\u0000\u0000\u0000\u02d4\u02d7\u0001"+
    "\u0000\u0000\u0000\u02d5\u02d3\u0001\u0000\u0000\u0000\u02d5\u02d6\u0001"+
    "\u0000\u0000\u0000\u02d6\u02d9\u0001\u0000\u0000\u0000\u02d7\u02d5\u0001"+
    "\u0000\u0000\u0000\u02d8\u02d0\u0001\u0000\u0000\u0000\u02d8\u02d9\u0001"+
    "\u0000\u0000\u0000\u02d9\u02da\u0001\u0000\u0000\u0000\u02da\u02db\u0005"+
    "]\u0000\u0000\u02db\u0091\u0001\u0000\u0000\u0000\u02dc\u02dd\u0003\u009e"+
    "O\u0000\u02dd\u02de\u0005=\u0000\u0000\u02de\u02df\u0003\u0094J\u0000"+
    "\u02df\u0093\u0001\u0000\u0000\u0000\u02e0\u030b\u0005H\u0000\u0000\u02e1"+
    "\u02e2\u0003\u009cN\u0000\u02e2\u02e3\u0005e\u0000\u0000\u02e3\u030b\u0001"+
    "\u0000\u0000\u0000\u02e4\u030b\u0003\u009aM\u0000\u02e5\u030b\u0003\u009c"+
    "N\u0000\u02e6\u030b\u0003\u0096K\u0000\u02e7\u030b\u0003:\u001d\u0000"+
    "\u02e8\u030b\u0003\u009eO\u0000\u02e9\u02ea\u0005a\u0000\u0000\u02ea\u02ef"+
    "\u0003\u0098L\u0000\u02eb\u02ec\u0005>\u0000\u0000\u02ec\u02ee\u0003\u0098"+
    "L\u0000\u02ed\u02eb\u0001\u0000\u0000\u0000\u02ee\u02f1\u0001\u0000\u0000"+
    "\u0000\u02ef\u02ed\u0001\u0000\u0000\u0000\u02ef\u02f0\u0001\u0000\u0000"+
    "\u0000\u02f0\u02f2\u0001\u0000\u0000\u0000\u02f1\u02ef\u0001\u0000\u0000"+
    "\u0000\u02f2\u02f3\u0005b\u0000\u0000\u02f3\u030b\u0001\u0000\u0000\u0000"+
    "\u02f4\u02f5\u0005a\u0000\u0000\u02f5\u02fa\u0003\u0096K\u0000\u02f6\u02f7"+
    "\u0005>\u0000\u0000\u02f7\u02f9\u0003\u0096K\u0000\u02f8\u02f6\u0001\u0000"+
    "\u0000\u0000\u02f9\u02fc\u0001\u0000\u0000\u0000\u02fa\u02f8\u0001\u0000"+
    "\u0000\u0000\u02fa\u02fb\u0001\u0000\u0000\u0000\u02fb\u02fd\u0001\u0000"+
    "\u0000\u0000\u02fc\u02fa\u0001\u0000\u0000\u0000\u02fd\u02fe\u0005b\u0000"+
    "\u0000\u02fe\u030b\u0001\u0000\u0000\u0000\u02ff\u0300\u0005a\u0000\u0000"+
    "\u0300\u0305\u0003\u009eO\u0000\u0301\u0302\u0005>\u0000\u0000\u0302\u0304"+
    "\u0003\u009eO\u0000\u0303\u0301\u0001\u0000\u0000\u0000\u0304\u0307\u0001"+
    "\u0000\u0000\u0000\u0305\u0303\u0001\u0000\u0000\u0000\u0305\u0306\u0001"+
    "\u0000\u0000\u0000\u0306\u0308\u0001\u0000\u0000\u0000\u0307\u0305\u0001"+
    "\u0000\u0000\u0000\u0308\u0309\u0005b\u0000\u0000\u0309\u030b\u0001\u0000"+
    "\u0000\u0000\u030a\u02e0\u0001\u0000\u0000\u0000\u030a\u02e1\u0001\u0000"+
    "\u0000\u0000\u030a\u02e4\u0001\u0000\u0000\u0000\u030a\u02e5\u0001\u0000"+
    "\u0000\u0000\u030a\u02e6\u0001\u0000\u0000\u0000\u030a\u02e7\u0001\u0000"+
    "\u0000\u0000\u030a\u02e8\u0001\u0000\u0000\u0000\u030a\u02e9\u0001\u0000"+
    "\u0000\u0000\u030a\u02f4\u0001\u0000\u0000\u0000\u030a\u02ff\u0001\u0000"+
    "\u0000\u0000\u030b\u0095\u0001\u0000\u0000\u0000\u030c\u030d\u0007\u0007"+
    "\u0000\u0000\u030d\u0097\u0001\u0000\u0000\u0000\u030e\u0311\u0003\u009a"+
    "M\u0000\u030f\u0311\u0003\u009cN\u0000\u0310\u030e\u0001\u0000\u0000\u0000"+
    "\u0310\u030f\u0001\u0000\u0000\u0000\u0311\u0099\u0001\u0000\u0000\u0000"+
    "\u0312\u0314\u0007\u0005\u0000\u0000\u0313\u0312\u0001\u0000\u0000\u0000"+
    "\u0313\u0314\u0001\u0000\u0000\u0000\u0314\u0315\u0001\u0000\u0000\u0000"+
    "\u0315\u0316\u00057\u0000\u0000\u0316\u009b\u0001\u0000\u0000\u0000\u0317"+
    "\u0319\u0007\u0005\u0000\u0000\u0318\u0317\u0001\u0000\u0000\u0000\u0318"+
    "\u0319\u0001\u0000\u0000\u0000\u0319\u031a\u0001\u0000\u0000\u0000\u031a"+
    "\u031b\u00056\u0000\u0000\u031b\u009d\u0001\u0000\u0000\u0000\u031c\u031d"+
    "\u00055\u0000\u0000\u031d\u009f\u0001\u0000\u0000\u0000\u031e\u031f\u0007"+
    "\b\u0000\u0000\u031f\u00a1\u0001\u0000\u0000\u0000\u0320\u0321\u0007\t"+
    "\u0000\u0000\u0321\u0322\u0005r\u0000\u0000\u0322\u0323\u0003\u00a4R\u0000"+
    "\u0323\u0324\u0003\u00a6S\u0000\u0324\u00a3\u0001\u0000\u0000\u0000\u0325"+
    "\u0326\u0003\u001c\u000e\u0000\u0326\u00a5\u0001\u0000\u0000\u0000\u0327"+
    "\u0328\u0005J\u0000\u0000\u0328\u032d\u0003\u00a8T\u0000\u0329\u032a\u0005"+
    ">\u0000\u0000\u032a\u032c\u0003\u00a8T\u0000\u032b\u0329\u0001\u0000\u0000"+
    "\u0000\u032c\u032f\u0001\u0000\u0000\u0000\u032d\u032b\u0001\u0000\u0000"+
    "\u0000\u032d\u032e\u0001\u0000\u0000\u0000\u032e\u00a7\u0001\u0000\u0000"+
    "\u0000\u032f\u032d\u0001\u0000\u0000\u0000\u0330\u0331\u0003\u0086C\u0000"+
    "\u0331\u00a9\u0001\u0000\u0000\u0000J\u00b5\u00bf\u00dc\u00eb\u00f1\u00fa"+
    "\u0100\u010d\u0111\u011c\u012c\u0134\u0138\u013f\u0145\u014c\u0154\u015c"+
    "\u0164\u0168\u016c\u0171\u017c\u0181\u0185\u0193\u019e\u01a4\u01ab\u01b4"+
    "\u01cb\u01d3\u01d6\u01dd\u01e8\u01ef\u01f7\u0205\u020e\u021c\u0227\u0235"+
    "\u023e\u0246\u024b\u0253\u0255\u025a\u0261\u0268\u0271\u0278\u0281\u0286"+
    "\u028b\u0295\u029b\u02a3\u02a5\u02b0\u02b7\u02c2\u02c7\u02c9\u02d5\u02d8"+
    "\u02ef\u02fa\u0305\u030a\u0310\u0313\u0318\u032d";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}

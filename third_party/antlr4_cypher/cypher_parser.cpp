
// Generated from Cypher.g4 by ANTLR 4.9



#include "cypher_parser.h"


using namespace antlrcpp;
using namespace antlr4;

CypherParser::CypherParser(TokenStream *input) : Parser(input) {
  _interpreter = new atn::ParserATNSimulator(this, _atn, _decisionToDFA, _sharedContextCache);
}

CypherParser::~CypherParser() {
  delete _interpreter;
}

std::string CypherParser::getGrammarFileName() const {
  return "Cypher.g4";
}

const std::vector<std::string>& CypherParser::getRuleNames() const {
  return _ruleNames;
}

dfa::Vocabulary& CypherParser::getVocabulary() const {
  return _vocabulary;
}


//----------------- OC_CypherContext ------------------------------------------------------------------

CypherParser::OC_CypherContext::OC_CypherContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_CypherContext::EOF() {
  return getToken(CypherParser::EOF, 0);
}

CypherParser::OC_StatementContext* CypherParser::OC_CypherContext::oC_Statement() {
  return getRuleContext<CypherParser::OC_StatementContext>(0);
}

CypherParser::KU_DDLContext* CypherParser::OC_CypherContext::kU_DDL() {
  return getRuleContext<CypherParser::KU_DDLContext>(0);
}

CypherParser::KU_CopyNPYContext* CypherParser::OC_CypherContext::kU_CopyNPY() {
  return getRuleContext<CypherParser::KU_CopyNPYContext>(0);
}

CypherParser::KU_CopyCSVContext* CypherParser::OC_CypherContext::kU_CopyCSV() {
  return getRuleContext<CypherParser::KU_CopyCSVContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_CypherContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_CypherContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OC_AnyCypherOptionContext* CypherParser::OC_CypherContext::oC_AnyCypherOption() {
  return getRuleContext<CypherParser::OC_AnyCypherOptionContext>(0);
}


size_t CypherParser::OC_CypherContext::getRuleIndex() const {
  return CypherParser::RuleOC_Cypher;
}


CypherParser::OC_CypherContext* CypherParser::oC_Cypher() {
  OC_CypherContext *_localctx = _tracker.createInstance<OC_CypherContext>(_ctx, getState());
  enterRule(_localctx, 0, CypherParser::RuleOC_Cypher);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(233);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 0, _ctx)) {
    case 1: {
      setState(232);
      match(CypherParser::SP);
      break;
    }

    default:
      break;
    }
    setState(236);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::EXPLAIN

    || _la == CypherParser::PROFILE) {
      setState(235);
      oC_AnyCypherOption();
    }
    setState(239);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 2, _ctx)) {
    case 1: {
      setState(238);
      match(CypherParser::SP);
      break;
    }

    default:
      break;
    }
    setState(245);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 3, _ctx)) {
    case 1: {
      setState(241);
      oC_Statement();
      break;
    }

    case 2: {
      setState(242);
      kU_DDL();
      break;
    }

    case 3: {
      setState(243);
      kU_CopyNPY();
      break;
    }

    case 4: {
      setState(244);
      kU_CopyCSV();
      break;
    }

    default:
      break;
    }
    setState(251);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 5, _ctx)) {
    case 1: {
      setState(248);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(247);
        match(CypherParser::SP);
      }
      setState(250);
      match(CypherParser::T__0);
      break;
    }

    default:
      break;
    }
    setState(254);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(253);
      match(CypherParser::SP);
    }
    setState(256);
    match(CypherParser::EOF);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_CopyCSVContext ------------------------------------------------------------------

CypherParser::KU_CopyCSVContext::KU_CopyCSVContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_CopyCSVContext::COPY() {
  return getToken(CypherParser::COPY, 0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_CopyCSVContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_CopyCSVContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OC_SchemaNameContext* CypherParser::KU_CopyCSVContext::oC_SchemaName() {
  return getRuleContext<CypherParser::OC_SchemaNameContext>(0);
}

tree::TerminalNode* CypherParser::KU_CopyCSVContext::FROM() {
  return getToken(CypherParser::FROM, 0);
}

CypherParser::KU_FilePathsContext* CypherParser::KU_CopyCSVContext::kU_FilePaths() {
  return getRuleContext<CypherParser::KU_FilePathsContext>(0);
}

CypherParser::KU_ParsingOptionsContext* CypherParser::KU_CopyCSVContext::kU_ParsingOptions() {
  return getRuleContext<CypherParser::KU_ParsingOptionsContext>(0);
}


size_t CypherParser::KU_CopyCSVContext::getRuleIndex() const {
  return CypherParser::RuleKU_CopyCSV;
}


CypherParser::KU_CopyCSVContext* CypherParser::kU_CopyCSV() {
  KU_CopyCSVContext *_localctx = _tracker.createInstance<KU_CopyCSVContext>(_ctx, getState());
  enterRule(_localctx, 2, CypherParser::RuleKU_CopyCSV);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(258);
    match(CypherParser::COPY);
    setState(259);
    match(CypherParser::SP);
    setState(260);
    oC_SchemaName();
    setState(261);
    match(CypherParser::SP);
    setState(262);
    match(CypherParser::FROM);
    setState(263);
    match(CypherParser::SP);
    setState(264);
    kU_FilePaths();
    setState(278);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 10, _ctx)) {
    case 1: {
      setState(266);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(265);
        match(CypherParser::SP);
      }
      setState(268);
      match(CypherParser::T__1);
      setState(270);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(269);
        match(CypherParser::SP);
      }
      setState(272);
      kU_ParsingOptions();
      setState(274);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(273);
        match(CypherParser::SP);
      }
      setState(276);
      match(CypherParser::T__2);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_CopyNPYContext ------------------------------------------------------------------

CypherParser::KU_CopyNPYContext::KU_CopyNPYContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_CopyNPYContext::COPY() {
  return getToken(CypherParser::COPY, 0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_CopyNPYContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_CopyNPYContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OC_SchemaNameContext* CypherParser::KU_CopyNPYContext::oC_SchemaName() {
  return getRuleContext<CypherParser::OC_SchemaNameContext>(0);
}

tree::TerminalNode* CypherParser::KU_CopyNPYContext::FROM() {
  return getToken(CypherParser::FROM, 0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_CopyNPYContext::StringLiteral() {
  return getTokens(CypherParser::StringLiteral);
}

tree::TerminalNode* CypherParser::KU_CopyNPYContext::StringLiteral(size_t i) {
  return getToken(CypherParser::StringLiteral, i);
}

tree::TerminalNode* CypherParser::KU_CopyNPYContext::BY() {
  return getToken(CypherParser::BY, 0);
}

tree::TerminalNode* CypherParser::KU_CopyNPYContext::COLUMN() {
  return getToken(CypherParser::COLUMN, 0);
}


size_t CypherParser::KU_CopyNPYContext::getRuleIndex() const {
  return CypherParser::RuleKU_CopyNPY;
}


CypherParser::KU_CopyNPYContext* CypherParser::kU_CopyNPY() {
  KU_CopyNPYContext *_localctx = _tracker.createInstance<KU_CopyNPYContext>(_ctx, getState());
  enterRule(_localctx, 4, CypherParser::RuleKU_CopyNPY);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(280);
    match(CypherParser::COPY);
    setState(281);
    match(CypherParser::SP);
    setState(282);
    oC_SchemaName();
    setState(283);
    match(CypherParser::SP);
    setState(284);
    match(CypherParser::FROM);
    setState(285);
    match(CypherParser::SP);
    setState(286);
    match(CypherParser::T__1);
    setState(288);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(287);
      match(CypherParser::SP);
    }
    setState(290);
    match(CypherParser::StringLiteral);
    setState(301);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == CypherParser::T__3 || _la == CypherParser::SP) {
      setState(292);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(291);
        match(CypherParser::SP);
      }
      setState(294);
      match(CypherParser::T__3);
      setState(296);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(295);
        match(CypherParser::SP);
      }
      setState(298);
      match(CypherParser::StringLiteral);
      setState(303);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(304);
    match(CypherParser::T__2);
    setState(305);
    match(CypherParser::SP);
    setState(306);
    match(CypherParser::BY);
    setState(307);
    match(CypherParser::SP);
    setState(308);
    match(CypherParser::COLUMN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_FilePathsContext ------------------------------------------------------------------

CypherParser::KU_FilePathsContext::KU_FilePathsContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<tree::TerminalNode *> CypherParser::KU_FilePathsContext::StringLiteral() {
  return getTokens(CypherParser::StringLiteral);
}

tree::TerminalNode* CypherParser::KU_FilePathsContext::StringLiteral(size_t i) {
  return getToken(CypherParser::StringLiteral, i);
}

std::vector<tree::TerminalNode *> CypherParser::KU_FilePathsContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_FilePathsContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::KU_FilePathsContext::GLOB() {
  return getToken(CypherParser::GLOB, 0);
}


size_t CypherParser::KU_FilePathsContext::getRuleIndex() const {
  return CypherParser::RuleKU_FilePaths;
}


CypherParser::KU_FilePathsContext* CypherParser::kU_FilePaths() {
  KU_FilePathsContext *_localctx = _tracker.createInstance<KU_FilePathsContext>(_ctx, getState());
  enterRule(_localctx, 6, CypherParser::RuleKU_FilePaths);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(343);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::T__4: {
        enterOuterAlt(_localctx, 1);
        setState(310);
        match(CypherParser::T__4);
        setState(312);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(311);
          match(CypherParser::SP);
        }
        setState(314);
        match(CypherParser::StringLiteral);
        setState(325);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == CypherParser::T__3 || _la == CypherParser::SP) {
          setState(316);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == CypherParser::SP) {
            setState(315);
            match(CypherParser::SP);
          }
          setState(318);
          match(CypherParser::T__3);
          setState(320);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == CypherParser::SP) {
            setState(319);
            match(CypherParser::SP);
          }
          setState(322);
          match(CypherParser::StringLiteral);
          setState(327);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(328);
        match(CypherParser::T__5);
        break;
      }

      case CypherParser::StringLiteral: {
        enterOuterAlt(_localctx, 2);
        setState(329);
        match(CypherParser::StringLiteral);
        break;
      }

      case CypherParser::GLOB: {
        enterOuterAlt(_localctx, 3);
        setState(330);
        match(CypherParser::GLOB);
        setState(332);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(331);
          match(CypherParser::SP);
        }
        setState(334);
        match(CypherParser::T__1);
        setState(336);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(335);
          match(CypherParser::SP);
        }
        setState(338);
        match(CypherParser::StringLiteral);
        setState(340);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(339);
          match(CypherParser::SP);
        }
        setState(342);
        match(CypherParser::T__2);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_ParsingOptionsContext ------------------------------------------------------------------

CypherParser::KU_ParsingOptionsContext::KU_ParsingOptionsContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::KU_ParsingOptionContext *> CypherParser::KU_ParsingOptionsContext::kU_ParsingOption() {
  return getRuleContexts<CypherParser::KU_ParsingOptionContext>();
}

CypherParser::KU_ParsingOptionContext* CypherParser::KU_ParsingOptionsContext::kU_ParsingOption(size_t i) {
  return getRuleContext<CypherParser::KU_ParsingOptionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::KU_ParsingOptionsContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_ParsingOptionsContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::KU_ParsingOptionsContext::getRuleIndex() const {
  return CypherParser::RuleKU_ParsingOptions;
}


CypherParser::KU_ParsingOptionsContext* CypherParser::kU_ParsingOptions() {
  KU_ParsingOptionsContext *_localctx = _tracker.createInstance<KU_ParsingOptionsContext>(_ctx, getState());
  enterRule(_localctx, 8, CypherParser::RuleKU_ParsingOptions);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(345);
    kU_ParsingOption();
    setState(356);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 25, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(347);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(346);
          match(CypherParser::SP);
        }
        setState(349);
        match(CypherParser::T__3);
        setState(351);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(350);
          match(CypherParser::SP);
        }
        setState(353);
        kU_ParsingOption(); 
      }
      setState(358);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 25, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_ParsingOptionContext ------------------------------------------------------------------

CypherParser::KU_ParsingOptionContext::KU_ParsingOptionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SymbolicNameContext* CypherParser::KU_ParsingOptionContext::oC_SymbolicName() {
  return getRuleContext<CypherParser::OC_SymbolicNameContext>(0);
}

CypherParser::OC_LiteralContext* CypherParser::KU_ParsingOptionContext::oC_Literal() {
  return getRuleContext<CypherParser::OC_LiteralContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_ParsingOptionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_ParsingOptionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::KU_ParsingOptionContext::getRuleIndex() const {
  return CypherParser::RuleKU_ParsingOption;
}


CypherParser::KU_ParsingOptionContext* CypherParser::kU_ParsingOption() {
  KU_ParsingOptionContext *_localctx = _tracker.createInstance<KU_ParsingOptionContext>(_ctx, getState());
  enterRule(_localctx, 10, CypherParser::RuleKU_ParsingOption);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(359);
    oC_SymbolicName();
    setState(361);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(360);
      match(CypherParser::SP);
    }
    setState(363);
    match(CypherParser::T__6);
    setState(365);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(364);
      match(CypherParser::SP);
    }
    setState(367);
    oC_Literal();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_DDLContext ------------------------------------------------------------------

CypherParser::KU_DDLContext::KU_DDLContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::KU_CreateNodeContext* CypherParser::KU_DDLContext::kU_CreateNode() {
  return getRuleContext<CypherParser::KU_CreateNodeContext>(0);
}

CypherParser::KU_CreateRelContext* CypherParser::KU_DDLContext::kU_CreateRel() {
  return getRuleContext<CypherParser::KU_CreateRelContext>(0);
}

CypherParser::KU_DropTableContext* CypherParser::KU_DDLContext::kU_DropTable() {
  return getRuleContext<CypherParser::KU_DropTableContext>(0);
}

CypherParser::KU_AlterTableContext* CypherParser::KU_DDLContext::kU_AlterTable() {
  return getRuleContext<CypherParser::KU_AlterTableContext>(0);
}


size_t CypherParser::KU_DDLContext::getRuleIndex() const {
  return CypherParser::RuleKU_DDL;
}


CypherParser::KU_DDLContext* CypherParser::kU_DDL() {
  KU_DDLContext *_localctx = _tracker.createInstance<KU_DDLContext>(_ctx, getState());
  enterRule(_localctx, 12, CypherParser::RuleKU_DDL);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(373);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 28, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(369);
      kU_CreateNode();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(370);
      kU_CreateRel();
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(371);
      kU_DropTable();
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(372);
      kU_AlterTable();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_CreateNodeContext ------------------------------------------------------------------

CypherParser::KU_CreateNodeContext::KU_CreateNodeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_CreateNodeContext::CREATE() {
  return getToken(CypherParser::CREATE, 0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_CreateNodeContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_CreateNodeContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::KU_CreateNodeContext::NODE() {
  return getToken(CypherParser::NODE, 0);
}

tree::TerminalNode* CypherParser::KU_CreateNodeContext::TABLE() {
  return getToken(CypherParser::TABLE, 0);
}

CypherParser::OC_SchemaNameContext* CypherParser::KU_CreateNodeContext::oC_SchemaName() {
  return getRuleContext<CypherParser::OC_SchemaNameContext>(0);
}

CypherParser::KU_PropertyDefinitionsContext* CypherParser::KU_CreateNodeContext::kU_PropertyDefinitions() {
  return getRuleContext<CypherParser::KU_PropertyDefinitionsContext>(0);
}

CypherParser::KU_CreateNodeConstraintContext* CypherParser::KU_CreateNodeContext::kU_CreateNodeConstraint() {
  return getRuleContext<CypherParser::KU_CreateNodeConstraintContext>(0);
}


size_t CypherParser::KU_CreateNodeContext::getRuleIndex() const {
  return CypherParser::RuleKU_CreateNode;
}


CypherParser::KU_CreateNodeContext* CypherParser::kU_CreateNode() {
  KU_CreateNodeContext *_localctx = _tracker.createInstance<KU_CreateNodeContext>(_ctx, getState());
  enterRule(_localctx, 14, CypherParser::RuleKU_CreateNode);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(375);
    match(CypherParser::CREATE);
    setState(376);
    match(CypherParser::SP);
    setState(377);
    match(CypherParser::NODE);
    setState(378);
    match(CypherParser::SP);
    setState(379);
    match(CypherParser::TABLE);
    setState(380);
    match(CypherParser::SP);
    setState(381);
    oC_SchemaName();
    setState(383);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(382);
      match(CypherParser::SP);
    }
    setState(385);
    match(CypherParser::T__1);
    setState(387);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(386);
      match(CypherParser::SP);
    }
    setState(389);
    kU_PropertyDefinitions();
    setState(391);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(390);
      match(CypherParser::SP);
    }

    setState(393);
    match(CypherParser::T__3);
    setState(395);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(394);
      match(CypherParser::SP);
    }
    setState(397);
    kU_CreateNodeConstraint();
    setState(400);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(399);
      match(CypherParser::SP);
    }
    setState(402);
    match(CypherParser::T__2);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_CreateRelContext ------------------------------------------------------------------

CypherParser::KU_CreateRelContext::KU_CreateRelContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_CreateRelContext::CREATE() {
  return getToken(CypherParser::CREATE, 0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_CreateRelContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_CreateRelContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::KU_CreateRelContext::REL() {
  return getToken(CypherParser::REL, 0);
}

tree::TerminalNode* CypherParser::KU_CreateRelContext::TABLE() {
  return getToken(CypherParser::TABLE, 0);
}

std::vector<CypherParser::OC_SchemaNameContext *> CypherParser::KU_CreateRelContext::oC_SchemaName() {
  return getRuleContexts<CypherParser::OC_SchemaNameContext>();
}

CypherParser::OC_SchemaNameContext* CypherParser::KU_CreateRelContext::oC_SchemaName(size_t i) {
  return getRuleContext<CypherParser::OC_SchemaNameContext>(i);
}

tree::TerminalNode* CypherParser::KU_CreateRelContext::FROM() {
  return getToken(CypherParser::FROM, 0);
}

tree::TerminalNode* CypherParser::KU_CreateRelContext::TO() {
  return getToken(CypherParser::TO, 0);
}

CypherParser::KU_PropertyDefinitionsContext* CypherParser::KU_CreateRelContext::kU_PropertyDefinitions() {
  return getRuleContext<CypherParser::KU_PropertyDefinitionsContext>(0);
}

CypherParser::OC_SymbolicNameContext* CypherParser::KU_CreateRelContext::oC_SymbolicName() {
  return getRuleContext<CypherParser::OC_SymbolicNameContext>(0);
}


size_t CypherParser::KU_CreateRelContext::getRuleIndex() const {
  return CypherParser::RuleKU_CreateRel;
}


CypherParser::KU_CreateRelContext* CypherParser::kU_CreateRel() {
  KU_CreateRelContext *_localctx = _tracker.createInstance<KU_CreateRelContext>(_ctx, getState());
  enterRule(_localctx, 16, CypherParser::RuleKU_CreateRel);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(404);
    match(CypherParser::CREATE);
    setState(405);
    match(CypherParser::SP);
    setState(406);
    match(CypherParser::REL);
    setState(407);
    match(CypherParser::SP);
    setState(408);
    match(CypherParser::TABLE);
    setState(409);
    match(CypherParser::SP);
    setState(410);
    oC_SchemaName();
    setState(412);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(411);
      match(CypherParser::SP);
    }
    setState(414);
    match(CypherParser::T__1);
    setState(416);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(415);
      match(CypherParser::SP);
    }
    setState(418);
    match(CypherParser::FROM);
    setState(419);
    match(CypherParser::SP);
    setState(420);
    oC_SchemaName();
    setState(421);
    match(CypherParser::SP);
    setState(422);
    match(CypherParser::TO);
    setState(423);
    match(CypherParser::SP);
    setState(424);
    oC_SchemaName();
    setState(426);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(425);
      match(CypherParser::SP);
    }
    setState(436);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 39, _ctx)) {
    case 1: {
      setState(428);
      match(CypherParser::T__3);
      setState(430);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(429);
        match(CypherParser::SP);
      }
      setState(432);
      kU_PropertyDefinitions();
      setState(434);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(433);
        match(CypherParser::SP);
      }
      break;
    }

    default:
      break;
    }
    setState(446);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::T__3) {
      setState(438);
      match(CypherParser::T__3);
      setState(440);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(439);
        match(CypherParser::SP);
      }
      setState(442);
      oC_SymbolicName();
      setState(444);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(443);
        match(CypherParser::SP);
      }
    }
    setState(448);
    match(CypherParser::T__2);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_DropTableContext ------------------------------------------------------------------

CypherParser::KU_DropTableContext::KU_DropTableContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_DropTableContext::DROP() {
  return getToken(CypherParser::DROP, 0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_DropTableContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_DropTableContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::KU_DropTableContext::TABLE() {
  return getToken(CypherParser::TABLE, 0);
}

CypherParser::OC_SchemaNameContext* CypherParser::KU_DropTableContext::oC_SchemaName() {
  return getRuleContext<CypherParser::OC_SchemaNameContext>(0);
}


size_t CypherParser::KU_DropTableContext::getRuleIndex() const {
  return CypherParser::RuleKU_DropTable;
}


CypherParser::KU_DropTableContext* CypherParser::kU_DropTable() {
  KU_DropTableContext *_localctx = _tracker.createInstance<KU_DropTableContext>(_ctx, getState());
  enterRule(_localctx, 18, CypherParser::RuleKU_DropTable);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(450);
    match(CypherParser::DROP);
    setState(451);
    match(CypherParser::SP);
    setState(452);
    match(CypherParser::TABLE);
    setState(453);
    match(CypherParser::SP);
    setState(454);
    oC_SchemaName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_AlterTableContext ------------------------------------------------------------------

CypherParser::KU_AlterTableContext::KU_AlterTableContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_AlterTableContext::ALTER() {
  return getToken(CypherParser::ALTER, 0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_AlterTableContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_AlterTableContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::KU_AlterTableContext::TABLE() {
  return getToken(CypherParser::TABLE, 0);
}

CypherParser::OC_SchemaNameContext* CypherParser::KU_AlterTableContext::oC_SchemaName() {
  return getRuleContext<CypherParser::OC_SchemaNameContext>(0);
}

CypherParser::KU_AlterOptionsContext* CypherParser::KU_AlterTableContext::kU_AlterOptions() {
  return getRuleContext<CypherParser::KU_AlterOptionsContext>(0);
}


size_t CypherParser::KU_AlterTableContext::getRuleIndex() const {
  return CypherParser::RuleKU_AlterTable;
}


CypherParser::KU_AlterTableContext* CypherParser::kU_AlterTable() {
  KU_AlterTableContext *_localctx = _tracker.createInstance<KU_AlterTableContext>(_ctx, getState());
  enterRule(_localctx, 20, CypherParser::RuleKU_AlterTable);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(456);
    match(CypherParser::ALTER);
    setState(457);
    match(CypherParser::SP);
    setState(458);
    match(CypherParser::TABLE);
    setState(459);
    match(CypherParser::SP);
    setState(460);
    oC_SchemaName();
    setState(461);
    match(CypherParser::SP);
    setState(462);
    kU_AlterOptions();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_AlterOptionsContext ------------------------------------------------------------------

CypherParser::KU_AlterOptionsContext::KU_AlterOptionsContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::KU_AddPropertyContext* CypherParser::KU_AlterOptionsContext::kU_AddProperty() {
  return getRuleContext<CypherParser::KU_AddPropertyContext>(0);
}

CypherParser::KU_DropPropertyContext* CypherParser::KU_AlterOptionsContext::kU_DropProperty() {
  return getRuleContext<CypherParser::KU_DropPropertyContext>(0);
}

CypherParser::KU_RenameTableContext* CypherParser::KU_AlterOptionsContext::kU_RenameTable() {
  return getRuleContext<CypherParser::KU_RenameTableContext>(0);
}

CypherParser::KU_RenamePropertyContext* CypherParser::KU_AlterOptionsContext::kU_RenameProperty() {
  return getRuleContext<CypherParser::KU_RenamePropertyContext>(0);
}


size_t CypherParser::KU_AlterOptionsContext::getRuleIndex() const {
  return CypherParser::RuleKU_AlterOptions;
}


CypherParser::KU_AlterOptionsContext* CypherParser::kU_AlterOptions() {
  KU_AlterOptionsContext *_localctx = _tracker.createInstance<KU_AlterOptionsContext>(_ctx, getState());
  enterRule(_localctx, 22, CypherParser::RuleKU_AlterOptions);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(468);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 43, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(464);
      kU_AddProperty();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(465);
      kU_DropProperty();
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(466);
      kU_RenameTable();
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(467);
      kU_RenameProperty();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_AddPropertyContext ------------------------------------------------------------------

CypherParser::KU_AddPropertyContext::KU_AddPropertyContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_AddPropertyContext::ADD() {
  return getToken(CypherParser::ADD, 0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_AddPropertyContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_AddPropertyContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OC_PropertyKeyNameContext* CypherParser::KU_AddPropertyContext::oC_PropertyKeyName() {
  return getRuleContext<CypherParser::OC_PropertyKeyNameContext>(0);
}

CypherParser::KU_DataTypeContext* CypherParser::KU_AddPropertyContext::kU_DataType() {
  return getRuleContext<CypherParser::KU_DataTypeContext>(0);
}

tree::TerminalNode* CypherParser::KU_AddPropertyContext::DEFAULT() {
  return getToken(CypherParser::DEFAULT, 0);
}

CypherParser::OC_ExpressionContext* CypherParser::KU_AddPropertyContext::oC_Expression() {
  return getRuleContext<CypherParser::OC_ExpressionContext>(0);
}


size_t CypherParser::KU_AddPropertyContext::getRuleIndex() const {
  return CypherParser::RuleKU_AddProperty;
}


CypherParser::KU_AddPropertyContext* CypherParser::kU_AddProperty() {
  KU_AddPropertyContext *_localctx = _tracker.createInstance<KU_AddPropertyContext>(_ctx, getState());
  enterRule(_localctx, 24, CypherParser::RuleKU_AddProperty);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(470);
    match(CypherParser::ADD);
    setState(471);
    match(CypherParser::SP);
    setState(472);
    oC_PropertyKeyName();
    setState(473);
    match(CypherParser::SP);
    setState(474);
    kU_DataType();
    setState(479);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 44, _ctx)) {
    case 1: {
      setState(475);
      match(CypherParser::SP);
      setState(476);
      match(CypherParser::DEFAULT);
      setState(477);
      match(CypherParser::SP);
      setState(478);
      oC_Expression();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_DropPropertyContext ------------------------------------------------------------------

CypherParser::KU_DropPropertyContext::KU_DropPropertyContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_DropPropertyContext::DROP() {
  return getToken(CypherParser::DROP, 0);
}

tree::TerminalNode* CypherParser::KU_DropPropertyContext::SP() {
  return getToken(CypherParser::SP, 0);
}

CypherParser::OC_PropertyKeyNameContext* CypherParser::KU_DropPropertyContext::oC_PropertyKeyName() {
  return getRuleContext<CypherParser::OC_PropertyKeyNameContext>(0);
}


size_t CypherParser::KU_DropPropertyContext::getRuleIndex() const {
  return CypherParser::RuleKU_DropProperty;
}


CypherParser::KU_DropPropertyContext* CypherParser::kU_DropProperty() {
  KU_DropPropertyContext *_localctx = _tracker.createInstance<KU_DropPropertyContext>(_ctx, getState());
  enterRule(_localctx, 26, CypherParser::RuleKU_DropProperty);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(481);
    match(CypherParser::DROP);
    setState(482);
    match(CypherParser::SP);
    setState(483);
    oC_PropertyKeyName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_RenameTableContext ------------------------------------------------------------------

CypherParser::KU_RenameTableContext::KU_RenameTableContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_RenameTableContext::RENAME() {
  return getToken(CypherParser::RENAME, 0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_RenameTableContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_RenameTableContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::KU_RenameTableContext::TO() {
  return getToken(CypherParser::TO, 0);
}

CypherParser::OC_SchemaNameContext* CypherParser::KU_RenameTableContext::oC_SchemaName() {
  return getRuleContext<CypherParser::OC_SchemaNameContext>(0);
}


size_t CypherParser::KU_RenameTableContext::getRuleIndex() const {
  return CypherParser::RuleKU_RenameTable;
}


CypherParser::KU_RenameTableContext* CypherParser::kU_RenameTable() {
  KU_RenameTableContext *_localctx = _tracker.createInstance<KU_RenameTableContext>(_ctx, getState());
  enterRule(_localctx, 28, CypherParser::RuleKU_RenameTable);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(485);
    match(CypherParser::RENAME);
    setState(486);
    match(CypherParser::SP);
    setState(487);
    match(CypherParser::TO);
    setState(488);
    match(CypherParser::SP);
    setState(489);
    oC_SchemaName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_RenamePropertyContext ------------------------------------------------------------------

CypherParser::KU_RenamePropertyContext::KU_RenamePropertyContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_RenamePropertyContext::RENAME() {
  return getToken(CypherParser::RENAME, 0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_RenamePropertyContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_RenamePropertyContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<CypherParser::OC_PropertyKeyNameContext *> CypherParser::KU_RenamePropertyContext::oC_PropertyKeyName() {
  return getRuleContexts<CypherParser::OC_PropertyKeyNameContext>();
}

CypherParser::OC_PropertyKeyNameContext* CypherParser::KU_RenamePropertyContext::oC_PropertyKeyName(size_t i) {
  return getRuleContext<CypherParser::OC_PropertyKeyNameContext>(i);
}

tree::TerminalNode* CypherParser::KU_RenamePropertyContext::TO() {
  return getToken(CypherParser::TO, 0);
}


size_t CypherParser::KU_RenamePropertyContext::getRuleIndex() const {
  return CypherParser::RuleKU_RenameProperty;
}


CypherParser::KU_RenamePropertyContext* CypherParser::kU_RenameProperty() {
  KU_RenamePropertyContext *_localctx = _tracker.createInstance<KU_RenamePropertyContext>(_ctx, getState());
  enterRule(_localctx, 30, CypherParser::RuleKU_RenameProperty);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(491);
    match(CypherParser::RENAME);
    setState(492);
    match(CypherParser::SP);
    setState(493);
    oC_PropertyKeyName();
    setState(494);
    match(CypherParser::SP);
    setState(495);
    match(CypherParser::TO);
    setState(496);
    match(CypherParser::SP);
    setState(497);
    oC_PropertyKeyName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_PropertyDefinitionsContext ------------------------------------------------------------------

CypherParser::KU_PropertyDefinitionsContext::KU_PropertyDefinitionsContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::KU_PropertyDefinitionContext *> CypherParser::KU_PropertyDefinitionsContext::kU_PropertyDefinition() {
  return getRuleContexts<CypherParser::KU_PropertyDefinitionContext>();
}

CypherParser::KU_PropertyDefinitionContext* CypherParser::KU_PropertyDefinitionsContext::kU_PropertyDefinition(size_t i) {
  return getRuleContext<CypherParser::KU_PropertyDefinitionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::KU_PropertyDefinitionsContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_PropertyDefinitionsContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::KU_PropertyDefinitionsContext::getRuleIndex() const {
  return CypherParser::RuleKU_PropertyDefinitions;
}


CypherParser::KU_PropertyDefinitionsContext* CypherParser::kU_PropertyDefinitions() {
  KU_PropertyDefinitionsContext *_localctx = _tracker.createInstance<KU_PropertyDefinitionsContext>(_ctx, getState());
  enterRule(_localctx, 32, CypherParser::RuleKU_PropertyDefinitions);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(499);
    kU_PropertyDefinition();
    setState(510);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 47, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(501);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(500);
          match(CypherParser::SP);
        }
        setState(503);
        match(CypherParser::T__3);
        setState(505);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(504);
          match(CypherParser::SP);
        }
        setState(507);
        kU_PropertyDefinition(); 
      }
      setState(512);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 47, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_PropertyDefinitionContext ------------------------------------------------------------------

CypherParser::KU_PropertyDefinitionContext::KU_PropertyDefinitionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_PropertyKeyNameContext* CypherParser::KU_PropertyDefinitionContext::oC_PropertyKeyName() {
  return getRuleContext<CypherParser::OC_PropertyKeyNameContext>(0);
}

tree::TerminalNode* CypherParser::KU_PropertyDefinitionContext::SP() {
  return getToken(CypherParser::SP, 0);
}

CypherParser::KU_DataTypeContext* CypherParser::KU_PropertyDefinitionContext::kU_DataType() {
  return getRuleContext<CypherParser::KU_DataTypeContext>(0);
}


size_t CypherParser::KU_PropertyDefinitionContext::getRuleIndex() const {
  return CypherParser::RuleKU_PropertyDefinition;
}


CypherParser::KU_PropertyDefinitionContext* CypherParser::kU_PropertyDefinition() {
  KU_PropertyDefinitionContext *_localctx = _tracker.createInstance<KU_PropertyDefinitionContext>(_ctx, getState());
  enterRule(_localctx, 34, CypherParser::RuleKU_PropertyDefinition);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(513);
    oC_PropertyKeyName();
    setState(514);
    match(CypherParser::SP);
    setState(515);
    kU_DataType();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_CreateNodeConstraintContext ------------------------------------------------------------------

CypherParser::KU_CreateNodeConstraintContext::KU_CreateNodeConstraintContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_CreateNodeConstraintContext::PRIMARY() {
  return getToken(CypherParser::PRIMARY, 0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_CreateNodeConstraintContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_CreateNodeConstraintContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::KU_CreateNodeConstraintContext::KEY() {
  return getToken(CypherParser::KEY, 0);
}

CypherParser::OC_PropertyKeyNameContext* CypherParser::KU_CreateNodeConstraintContext::oC_PropertyKeyName() {
  return getRuleContext<CypherParser::OC_PropertyKeyNameContext>(0);
}


size_t CypherParser::KU_CreateNodeConstraintContext::getRuleIndex() const {
  return CypherParser::RuleKU_CreateNodeConstraint;
}


CypherParser::KU_CreateNodeConstraintContext* CypherParser::kU_CreateNodeConstraint() {
  KU_CreateNodeConstraintContext *_localctx = _tracker.createInstance<KU_CreateNodeConstraintContext>(_ctx, getState());
  enterRule(_localctx, 36, CypherParser::RuleKU_CreateNodeConstraint);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(517);
    match(CypherParser::PRIMARY);
    setState(518);
    match(CypherParser::SP);
    setState(519);
    match(CypherParser::KEY);
    setState(521);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(520);
      match(CypherParser::SP);
    }
    setState(523);
    match(CypherParser::T__1);
    setState(525);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(524);
      match(CypherParser::SP);
    }
    setState(527);
    oC_PropertyKeyName();
    setState(529);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(528);
      match(CypherParser::SP);
    }
    setState(531);
    match(CypherParser::T__2);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_DataTypeContext ------------------------------------------------------------------

CypherParser::KU_DataTypeContext::KU_DataTypeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SymbolicNameContext* CypherParser::KU_DataTypeContext::oC_SymbolicName() {
  return getRuleContext<CypherParser::OC_SymbolicNameContext>(0);
}

CypherParser::KU_ListIdentifiersContext* CypherParser::KU_DataTypeContext::kU_ListIdentifiers() {
  return getRuleContext<CypherParser::KU_ListIdentifiersContext>(0);
}

CypherParser::KU_PropertyDefinitionsContext* CypherParser::KU_DataTypeContext::kU_PropertyDefinitions() {
  return getRuleContext<CypherParser::KU_PropertyDefinitionsContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_DataTypeContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_DataTypeContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::KU_DataTypeContext::getRuleIndex() const {
  return CypherParser::RuleKU_DataType;
}


CypherParser::KU_DataTypeContext* CypherParser::kU_DataType() {
  KU_DataTypeContext *_localctx = _tracker.createInstance<KU_DataTypeContext>(_ctx, getState());
  enterRule(_localctx, 38, CypherParser::RuleKU_DataType);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(551);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 54, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(533);
      oC_SymbolicName();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(534);
      oC_SymbolicName();
      setState(535);
      kU_ListIdentifiers();
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(537);
      oC_SymbolicName();
      setState(539);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(538);
        match(CypherParser::SP);
      }
      setState(541);
      match(CypherParser::T__1);
      setState(543);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(542);
        match(CypherParser::SP);
      }
      setState(545);
      kU_PropertyDefinitions();
      setState(547);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(546);
        match(CypherParser::SP);
      }
      setState(549);
      match(CypherParser::T__2);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_ListIdentifiersContext ------------------------------------------------------------------

CypherParser::KU_ListIdentifiersContext::KU_ListIdentifiersContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::KU_ListIdentifierContext *> CypherParser::KU_ListIdentifiersContext::kU_ListIdentifier() {
  return getRuleContexts<CypherParser::KU_ListIdentifierContext>();
}

CypherParser::KU_ListIdentifierContext* CypherParser::KU_ListIdentifiersContext::kU_ListIdentifier(size_t i) {
  return getRuleContext<CypherParser::KU_ListIdentifierContext>(i);
}


size_t CypherParser::KU_ListIdentifiersContext::getRuleIndex() const {
  return CypherParser::RuleKU_ListIdentifiers;
}


CypherParser::KU_ListIdentifiersContext* CypherParser::kU_ListIdentifiers() {
  KU_ListIdentifiersContext *_localctx = _tracker.createInstance<KU_ListIdentifiersContext>(_ctx, getState());
  enterRule(_localctx, 40, CypherParser::RuleKU_ListIdentifiers);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(553);
    kU_ListIdentifier();
    setState(557);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == CypherParser::T__4) {
      setState(554);
      kU_ListIdentifier();
      setState(559);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_ListIdentifierContext ------------------------------------------------------------------

CypherParser::KU_ListIdentifierContext::KU_ListIdentifierContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_IntegerLiteralContext* CypherParser::KU_ListIdentifierContext::oC_IntegerLiteral() {
  return getRuleContext<CypherParser::OC_IntegerLiteralContext>(0);
}


size_t CypherParser::KU_ListIdentifierContext::getRuleIndex() const {
  return CypherParser::RuleKU_ListIdentifier;
}


CypherParser::KU_ListIdentifierContext* CypherParser::kU_ListIdentifier() {
  KU_ListIdentifierContext *_localctx = _tracker.createInstance<KU_ListIdentifierContext>(_ctx, getState());
  enterRule(_localctx, 42, CypherParser::RuleKU_ListIdentifier);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(560);
    match(CypherParser::T__4);
    setState(562);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::DecimalInteger) {
      setState(561);
      oC_IntegerLiteral();
    }
    setState(564);
    match(CypherParser::T__5);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_AnyCypherOptionContext ------------------------------------------------------------------

CypherParser::OC_AnyCypherOptionContext::OC_AnyCypherOptionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_ExplainContext* CypherParser::OC_AnyCypherOptionContext::oC_Explain() {
  return getRuleContext<CypherParser::OC_ExplainContext>(0);
}

CypherParser::OC_ProfileContext* CypherParser::OC_AnyCypherOptionContext::oC_Profile() {
  return getRuleContext<CypherParser::OC_ProfileContext>(0);
}


size_t CypherParser::OC_AnyCypherOptionContext::getRuleIndex() const {
  return CypherParser::RuleOC_AnyCypherOption;
}


CypherParser::OC_AnyCypherOptionContext* CypherParser::oC_AnyCypherOption() {
  OC_AnyCypherOptionContext *_localctx = _tracker.createInstance<OC_AnyCypherOptionContext>(_ctx, getState());
  enterRule(_localctx, 44, CypherParser::RuleOC_AnyCypherOption);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(568);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::EXPLAIN: {
        enterOuterAlt(_localctx, 1);
        setState(566);
        oC_Explain();
        break;
      }

      case CypherParser::PROFILE: {
        enterOuterAlt(_localctx, 2);
        setState(567);
        oC_Profile();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ExplainContext ------------------------------------------------------------------

CypherParser::OC_ExplainContext::OC_ExplainContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_ExplainContext::EXPLAIN() {
  return getToken(CypherParser::EXPLAIN, 0);
}


size_t CypherParser::OC_ExplainContext::getRuleIndex() const {
  return CypherParser::RuleOC_Explain;
}


CypherParser::OC_ExplainContext* CypherParser::oC_Explain() {
  OC_ExplainContext *_localctx = _tracker.createInstance<OC_ExplainContext>(_ctx, getState());
  enterRule(_localctx, 46, CypherParser::RuleOC_Explain);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(570);
    match(CypherParser::EXPLAIN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ProfileContext ------------------------------------------------------------------

CypherParser::OC_ProfileContext::OC_ProfileContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_ProfileContext::PROFILE() {
  return getToken(CypherParser::PROFILE, 0);
}


size_t CypherParser::OC_ProfileContext::getRuleIndex() const {
  return CypherParser::RuleOC_Profile;
}


CypherParser::OC_ProfileContext* CypherParser::oC_Profile() {
  OC_ProfileContext *_localctx = _tracker.createInstance<OC_ProfileContext>(_ctx, getState());
  enterRule(_localctx, 48, CypherParser::RuleOC_Profile);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(572);
    match(CypherParser::PROFILE);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_StatementContext ------------------------------------------------------------------

CypherParser::OC_StatementContext::OC_StatementContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_QueryContext* CypherParser::OC_StatementContext::oC_Query() {
  return getRuleContext<CypherParser::OC_QueryContext>(0);
}


size_t CypherParser::OC_StatementContext::getRuleIndex() const {
  return CypherParser::RuleOC_Statement;
}


CypherParser::OC_StatementContext* CypherParser::oC_Statement() {
  OC_StatementContext *_localctx = _tracker.createInstance<OC_StatementContext>(_ctx, getState());
  enterRule(_localctx, 50, CypherParser::RuleOC_Statement);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(574);
    oC_Query();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_QueryContext ------------------------------------------------------------------

CypherParser::OC_QueryContext::OC_QueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_RegularQueryContext* CypherParser::OC_QueryContext::oC_RegularQuery() {
  return getRuleContext<CypherParser::OC_RegularQueryContext>(0);
}


size_t CypherParser::OC_QueryContext::getRuleIndex() const {
  return CypherParser::RuleOC_Query;
}


CypherParser::OC_QueryContext* CypherParser::oC_Query() {
  OC_QueryContext *_localctx = _tracker.createInstance<OC_QueryContext>(_ctx, getState());
  enterRule(_localctx, 52, CypherParser::RuleOC_Query);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(576);
    oC_RegularQuery();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_RegularQueryContext ------------------------------------------------------------------

CypherParser::OC_RegularQueryContext::OC_RegularQueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SingleQueryContext* CypherParser::OC_RegularQueryContext::oC_SingleQuery() {
  return getRuleContext<CypherParser::OC_SingleQueryContext>(0);
}

std::vector<CypherParser::OC_UnionContext *> CypherParser::OC_RegularQueryContext::oC_Union() {
  return getRuleContexts<CypherParser::OC_UnionContext>();
}

CypherParser::OC_UnionContext* CypherParser::OC_RegularQueryContext::oC_Union(size_t i) {
  return getRuleContext<CypherParser::OC_UnionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_RegularQueryContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_RegularQueryContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<CypherParser::OC_ReturnContext *> CypherParser::OC_RegularQueryContext::oC_Return() {
  return getRuleContexts<CypherParser::OC_ReturnContext>();
}

CypherParser::OC_ReturnContext* CypherParser::OC_RegularQueryContext::oC_Return(size_t i) {
  return getRuleContext<CypherParser::OC_ReturnContext>(i);
}


size_t CypherParser::OC_RegularQueryContext::getRuleIndex() const {
  return CypherParser::RuleOC_RegularQuery;
}


CypherParser::OC_RegularQueryContext* CypherParser::oC_RegularQuery() {
  OC_RegularQueryContext *_localctx = _tracker.createInstance<OC_RegularQueryContext>(_ctx, getState());
  enterRule(_localctx, 54, CypherParser::RuleOC_RegularQuery);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    setState(599);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 62, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(578);
      oC_SingleQuery();
      setState(585);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 59, _ctx);
      while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
        if (alt == 1) {
          setState(580);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == CypherParser::SP) {
            setState(579);
            match(CypherParser::SP);
          }
          setState(582);
          oC_Union(); 
        }
        setState(587);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 59, _ctx);
      }
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(592); 
      _errHandler->sync(this);
      alt = 1;
      do {
        switch (alt) {
          case 1: {
                setState(588);
                oC_Return();
                setState(590);
                _errHandler->sync(this);

                switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 60, _ctx)) {
                case 1: {
                  setState(589);
                  match(CypherParser::SP);
                  break;
                }

                default:
                  break;
                }
                break;
              }

        default:
          throw NoViableAltException(this);
        }
        setState(594); 
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 61, _ctx);
      } while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER);
      setState(596);
      oC_SingleQuery();
       notifyReturnNotAtEnd(_localctx->start); 
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_UnionContext ------------------------------------------------------------------

CypherParser::OC_UnionContext::OC_UnionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_UnionContext::UNION() {
  return getToken(CypherParser::UNION, 0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_UnionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_UnionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::OC_UnionContext::ALL() {
  return getToken(CypherParser::ALL, 0);
}

CypherParser::OC_SingleQueryContext* CypherParser::OC_UnionContext::oC_SingleQuery() {
  return getRuleContext<CypherParser::OC_SingleQueryContext>(0);
}


size_t CypherParser::OC_UnionContext::getRuleIndex() const {
  return CypherParser::RuleOC_Union;
}


CypherParser::OC_UnionContext* CypherParser::oC_Union() {
  OC_UnionContext *_localctx = _tracker.createInstance<OC_UnionContext>(_ctx, getState());
  enterRule(_localctx, 56, CypherParser::RuleOC_Union);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(613);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 65, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(601);
      match(CypherParser::UNION);
      setState(602);
      match(CypherParser::SP);
      setState(603);
      match(CypherParser::ALL);
      setState(605);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 63, _ctx)) {
      case 1: {
        setState(604);
        match(CypherParser::SP);
        break;
      }

      default:
        break;
      }
      setState(607);
      oC_SingleQuery();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(608);
      match(CypherParser::UNION);
      setState(610);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 64, _ctx)) {
      case 1: {
        setState(609);
        match(CypherParser::SP);
        break;
      }

      default:
        break;
      }
      setState(612);
      oC_SingleQuery();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_SingleQueryContext ------------------------------------------------------------------

CypherParser::OC_SingleQueryContext::OC_SingleQueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SinglePartQueryContext* CypherParser::OC_SingleQueryContext::oC_SinglePartQuery() {
  return getRuleContext<CypherParser::OC_SinglePartQueryContext>(0);
}

CypherParser::OC_MultiPartQueryContext* CypherParser::OC_SingleQueryContext::oC_MultiPartQuery() {
  return getRuleContext<CypherParser::OC_MultiPartQueryContext>(0);
}


size_t CypherParser::OC_SingleQueryContext::getRuleIndex() const {
  return CypherParser::RuleOC_SingleQuery;
}


CypherParser::OC_SingleQueryContext* CypherParser::oC_SingleQuery() {
  OC_SingleQueryContext *_localctx = _tracker.createInstance<OC_SingleQueryContext>(_ctx, getState());
  enterRule(_localctx, 58, CypherParser::RuleOC_SingleQuery);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(617);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 66, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(615);
      oC_SinglePartQuery();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(616);
      oC_MultiPartQuery();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_SinglePartQueryContext ------------------------------------------------------------------

CypherParser::OC_SinglePartQueryContext::OC_SinglePartQueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_ReturnContext* CypherParser::OC_SinglePartQueryContext::oC_Return() {
  return getRuleContext<CypherParser::OC_ReturnContext>(0);
}

std::vector<CypherParser::OC_ReadingClauseContext *> CypherParser::OC_SinglePartQueryContext::oC_ReadingClause() {
  return getRuleContexts<CypherParser::OC_ReadingClauseContext>();
}

CypherParser::OC_ReadingClauseContext* CypherParser::OC_SinglePartQueryContext::oC_ReadingClause(size_t i) {
  return getRuleContext<CypherParser::OC_ReadingClauseContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_SinglePartQueryContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_SinglePartQueryContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<CypherParser::OC_UpdatingClauseContext *> CypherParser::OC_SinglePartQueryContext::oC_UpdatingClause() {
  return getRuleContexts<CypherParser::OC_UpdatingClauseContext>();
}

CypherParser::OC_UpdatingClauseContext* CypherParser::OC_SinglePartQueryContext::oC_UpdatingClause(size_t i) {
  return getRuleContext<CypherParser::OC_UpdatingClauseContext>(i);
}


size_t CypherParser::OC_SinglePartQueryContext::getRuleIndex() const {
  return CypherParser::RuleOC_SinglePartQuery;
}


CypherParser::OC_SinglePartQueryContext* CypherParser::oC_SinglePartQuery() {
  OC_SinglePartQueryContext *_localctx = _tracker.createInstance<OC_SinglePartQueryContext>(_ctx, getState());
  enterRule(_localctx, 60, CypherParser::RuleOC_SinglePartQuery);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    setState(664);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 77, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(625);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (((((_la - 67) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 67)) & ((1ULL << (CypherParser::OPTIONAL - 67))
        | (1ULL << (CypherParser::MATCH - 67))
        | (1ULL << (CypherParser::UNWIND - 67)))) != 0)) {
        setState(619);
        oC_ReadingClause();
        setState(621);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(620);
          match(CypherParser::SP);
        }
        setState(627);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(628);
      oC_Return();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(635);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (((((_la - 67) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 67)) & ((1ULL << (CypherParser::OPTIONAL - 67))
        | (1ULL << (CypherParser::MATCH - 67))
        | (1ULL << (CypherParser::UNWIND - 67)))) != 0)) {
        setState(629);
        oC_ReadingClause();
        setState(631);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(630);
          match(CypherParser::SP);
        }
        setState(637);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(638);
      oC_UpdatingClause();
      setState(645);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 72, _ctx);
      while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
        if (alt == 1) {
          setState(640);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == CypherParser::SP) {
            setState(639);
            match(CypherParser::SP);
          }
          setState(642);
          oC_UpdatingClause(); 
        }
        setState(647);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 72, _ctx);
      }
      setState(652);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 74, _ctx)) {
      case 1: {
        setState(649);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(648);
          match(CypherParser::SP);
        }
        setState(651);
        oC_Return();
        break;
      }

      default:
        break;
      }
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(660);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (((((_la - 67) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 67)) & ((1ULL << (CypherParser::OPTIONAL - 67))
        | (1ULL << (CypherParser::MATCH - 67))
        | (1ULL << (CypherParser::UNWIND - 67)))) != 0)) {
        setState(654);
        oC_ReadingClause();
        setState(656);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 75, _ctx)) {
        case 1: {
          setState(655);
          match(CypherParser::SP);
          break;
        }

        default:
          break;
        }
        setState(662);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
       notifyQueryNotConcludeWithReturn(_localctx->start); 
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_MultiPartQueryContext ------------------------------------------------------------------

CypherParser::OC_MultiPartQueryContext::OC_MultiPartQueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SinglePartQueryContext* CypherParser::OC_MultiPartQueryContext::oC_SinglePartQuery() {
  return getRuleContext<CypherParser::OC_SinglePartQueryContext>(0);
}

std::vector<CypherParser::KU_QueryPartContext *> CypherParser::OC_MultiPartQueryContext::kU_QueryPart() {
  return getRuleContexts<CypherParser::KU_QueryPartContext>();
}

CypherParser::KU_QueryPartContext* CypherParser::OC_MultiPartQueryContext::kU_QueryPart(size_t i) {
  return getRuleContext<CypherParser::KU_QueryPartContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_MultiPartQueryContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_MultiPartQueryContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_MultiPartQueryContext::getRuleIndex() const {
  return CypherParser::RuleOC_MultiPartQuery;
}


CypherParser::OC_MultiPartQueryContext* CypherParser::oC_MultiPartQuery() {
  OC_MultiPartQueryContext *_localctx = _tracker.createInstance<OC_MultiPartQueryContext>(_ctx, getState());
  enterRule(_localctx, 62, CypherParser::RuleOC_MultiPartQuery);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(670); 
    _errHandler->sync(this);
    alt = 1;
    do {
      switch (alt) {
        case 1: {
              setState(666);
              kU_QueryPart();
              setState(668);
              _errHandler->sync(this);

              switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 78, _ctx)) {
              case 1: {
                setState(667);
                match(CypherParser::SP);
                break;
              }

              default:
                break;
              }
              break;
            }

      default:
        throw NoViableAltException(this);
      }
      setState(672); 
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 79, _ctx);
    } while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER);
    setState(674);
    oC_SinglePartQuery();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_QueryPartContext ------------------------------------------------------------------

CypherParser::KU_QueryPartContext::KU_QueryPartContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_WithContext* CypherParser::KU_QueryPartContext::oC_With() {
  return getRuleContext<CypherParser::OC_WithContext>(0);
}

std::vector<CypherParser::OC_ReadingClauseContext *> CypherParser::KU_QueryPartContext::oC_ReadingClause() {
  return getRuleContexts<CypherParser::OC_ReadingClauseContext>();
}

CypherParser::OC_ReadingClauseContext* CypherParser::KU_QueryPartContext::oC_ReadingClause(size_t i) {
  return getRuleContext<CypherParser::OC_ReadingClauseContext>(i);
}

std::vector<CypherParser::OC_UpdatingClauseContext *> CypherParser::KU_QueryPartContext::oC_UpdatingClause() {
  return getRuleContexts<CypherParser::OC_UpdatingClauseContext>();
}

CypherParser::OC_UpdatingClauseContext* CypherParser::KU_QueryPartContext::oC_UpdatingClause(size_t i) {
  return getRuleContext<CypherParser::OC_UpdatingClauseContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::KU_QueryPartContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_QueryPartContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::KU_QueryPartContext::getRuleIndex() const {
  return CypherParser::RuleKU_QueryPart;
}


CypherParser::KU_QueryPartContext* CypherParser::kU_QueryPart() {
  KU_QueryPartContext *_localctx = _tracker.createInstance<KU_QueryPartContext>(_ctx, getState());
  enterRule(_localctx, 64, CypherParser::RuleKU_QueryPart);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(682);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (((((_la - 67) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 67)) & ((1ULL << (CypherParser::OPTIONAL - 67))
      | (1ULL << (CypherParser::MATCH - 67))
      | (1ULL << (CypherParser::UNWIND - 67)))) != 0)) {
      setState(676);
      oC_ReadingClause();
      setState(678);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(677);
        match(CypherParser::SP);
      }
      setState(684);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(691);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (((((_la - 70) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 70)) & ((1ULL << (CypherParser::CREATE - 70))
      | (1ULL << (CypherParser::SET - 70))
      | (1ULL << (CypherParser::DELETE - 70)))) != 0)) {
      setState(685);
      oC_UpdatingClause();
      setState(687);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(686);
        match(CypherParser::SP);
      }
      setState(693);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(694);
    oC_With();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_UpdatingClauseContext ------------------------------------------------------------------

CypherParser::OC_UpdatingClauseContext::OC_UpdatingClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_CreateContext* CypherParser::OC_UpdatingClauseContext::oC_Create() {
  return getRuleContext<CypherParser::OC_CreateContext>(0);
}

CypherParser::OC_SetContext* CypherParser::OC_UpdatingClauseContext::oC_Set() {
  return getRuleContext<CypherParser::OC_SetContext>(0);
}

CypherParser::OC_DeleteContext* CypherParser::OC_UpdatingClauseContext::oC_Delete() {
  return getRuleContext<CypherParser::OC_DeleteContext>(0);
}


size_t CypherParser::OC_UpdatingClauseContext::getRuleIndex() const {
  return CypherParser::RuleOC_UpdatingClause;
}


CypherParser::OC_UpdatingClauseContext* CypherParser::oC_UpdatingClause() {
  OC_UpdatingClauseContext *_localctx = _tracker.createInstance<OC_UpdatingClauseContext>(_ctx, getState());
  enterRule(_localctx, 66, CypherParser::RuleOC_UpdatingClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(699);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::CREATE: {
        enterOuterAlt(_localctx, 1);
        setState(696);
        oC_Create();
        break;
      }

      case CypherParser::SET: {
        enterOuterAlt(_localctx, 2);
        setState(697);
        oC_Set();
        break;
      }

      case CypherParser::DELETE: {
        enterOuterAlt(_localctx, 3);
        setState(698);
        oC_Delete();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ReadingClauseContext ------------------------------------------------------------------

CypherParser::OC_ReadingClauseContext::OC_ReadingClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_MatchContext* CypherParser::OC_ReadingClauseContext::oC_Match() {
  return getRuleContext<CypherParser::OC_MatchContext>(0);
}

CypherParser::OC_UnwindContext* CypherParser::OC_ReadingClauseContext::oC_Unwind() {
  return getRuleContext<CypherParser::OC_UnwindContext>(0);
}


size_t CypherParser::OC_ReadingClauseContext::getRuleIndex() const {
  return CypherParser::RuleOC_ReadingClause;
}


CypherParser::OC_ReadingClauseContext* CypherParser::oC_ReadingClause() {
  OC_ReadingClauseContext *_localctx = _tracker.createInstance<OC_ReadingClauseContext>(_ctx, getState());
  enterRule(_localctx, 68, CypherParser::RuleOC_ReadingClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(703);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::OPTIONAL:
      case CypherParser::MATCH: {
        enterOuterAlt(_localctx, 1);
        setState(701);
        oC_Match();
        break;
      }

      case CypherParser::UNWIND: {
        enterOuterAlt(_localctx, 2);
        setState(702);
        oC_Unwind();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_MatchContext ------------------------------------------------------------------

CypherParser::OC_MatchContext::OC_MatchContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_MatchContext::MATCH() {
  return getToken(CypherParser::MATCH, 0);
}

CypherParser::OC_PatternContext* CypherParser::OC_MatchContext::oC_Pattern() {
  return getRuleContext<CypherParser::OC_PatternContext>(0);
}

tree::TerminalNode* CypherParser::OC_MatchContext::OPTIONAL() {
  return getToken(CypherParser::OPTIONAL, 0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_MatchContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_MatchContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OC_WhereContext* CypherParser::OC_MatchContext::oC_Where() {
  return getRuleContext<CypherParser::OC_WhereContext>(0);
}


size_t CypherParser::OC_MatchContext::getRuleIndex() const {
  return CypherParser::RuleOC_Match;
}


CypherParser::OC_MatchContext* CypherParser::oC_Match() {
  OC_MatchContext *_localctx = _tracker.createInstance<OC_MatchContext>(_ctx, getState());
  enterRule(_localctx, 70, CypherParser::RuleOC_Match);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(707);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::OPTIONAL) {
      setState(705);
      match(CypherParser::OPTIONAL);
      setState(706);
      match(CypherParser::SP);
    }
    setState(709);
    match(CypherParser::MATCH);
    setState(711);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 87, _ctx)) {
    case 1: {
      setState(710);
      match(CypherParser::SP);
      break;
    }

    default:
      break;
    }
    setState(713);
    oC_Pattern();
    setState(718);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 89, _ctx)) {
    case 1: {
      setState(715);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(714);
        match(CypherParser::SP);
      }
      setState(717);
      oC_Where();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_UnwindContext ------------------------------------------------------------------

CypherParser::OC_UnwindContext::OC_UnwindContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_UnwindContext::UNWIND() {
  return getToken(CypherParser::UNWIND, 0);
}

CypherParser::OC_ExpressionContext* CypherParser::OC_UnwindContext::oC_Expression() {
  return getRuleContext<CypherParser::OC_ExpressionContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_UnwindContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_UnwindContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::OC_UnwindContext::AS() {
  return getToken(CypherParser::AS, 0);
}

CypherParser::OC_VariableContext* CypherParser::OC_UnwindContext::oC_Variable() {
  return getRuleContext<CypherParser::OC_VariableContext>(0);
}


size_t CypherParser::OC_UnwindContext::getRuleIndex() const {
  return CypherParser::RuleOC_Unwind;
}


CypherParser::OC_UnwindContext* CypherParser::oC_Unwind() {
  OC_UnwindContext *_localctx = _tracker.createInstance<OC_UnwindContext>(_ctx, getState());
  enterRule(_localctx, 72, CypherParser::RuleOC_Unwind);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(720);
    match(CypherParser::UNWIND);
    setState(722);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(721);
      match(CypherParser::SP);
    }
    setState(724);
    oC_Expression();
    setState(725);
    match(CypherParser::SP);
    setState(726);
    match(CypherParser::AS);
    setState(727);
    match(CypherParser::SP);
    setState(728);
    oC_Variable();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_CreateContext ------------------------------------------------------------------

CypherParser::OC_CreateContext::OC_CreateContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_CreateContext::CREATE() {
  return getToken(CypherParser::CREATE, 0);
}

CypherParser::OC_PatternContext* CypherParser::OC_CreateContext::oC_Pattern() {
  return getRuleContext<CypherParser::OC_PatternContext>(0);
}

tree::TerminalNode* CypherParser::OC_CreateContext::SP() {
  return getToken(CypherParser::SP, 0);
}


size_t CypherParser::OC_CreateContext::getRuleIndex() const {
  return CypherParser::RuleOC_Create;
}


CypherParser::OC_CreateContext* CypherParser::oC_Create() {
  OC_CreateContext *_localctx = _tracker.createInstance<OC_CreateContext>(_ctx, getState());
  enterRule(_localctx, 74, CypherParser::RuleOC_Create);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(730);
    match(CypherParser::CREATE);
    setState(732);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 91, _ctx)) {
    case 1: {
      setState(731);
      match(CypherParser::SP);
      break;
    }

    default:
      break;
    }
    setState(734);
    oC_Pattern();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_SetContext ------------------------------------------------------------------

CypherParser::OC_SetContext::OC_SetContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_SetContext::SET() {
  return getToken(CypherParser::SET, 0);
}

std::vector<CypherParser::OC_SetItemContext *> CypherParser::OC_SetContext::oC_SetItem() {
  return getRuleContexts<CypherParser::OC_SetItemContext>();
}

CypherParser::OC_SetItemContext* CypherParser::OC_SetContext::oC_SetItem(size_t i) {
  return getRuleContext<CypherParser::OC_SetItemContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_SetContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_SetContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_SetContext::getRuleIndex() const {
  return CypherParser::RuleOC_Set;
}


CypherParser::OC_SetContext* CypherParser::oC_Set() {
  OC_SetContext *_localctx = _tracker.createInstance<OC_SetContext>(_ctx, getState());
  enterRule(_localctx, 76, CypherParser::RuleOC_Set);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(736);
    match(CypherParser::SET);
    setState(738);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(737);
      match(CypherParser::SP);
    }
    setState(740);
    oC_SetItem();
    setState(751);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 95, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(742);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(741);
          match(CypherParser::SP);
        }
        setState(744);
        match(CypherParser::T__3);
        setState(746);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(745);
          match(CypherParser::SP);
        }
        setState(748);
        oC_SetItem(); 
      }
      setState(753);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 95, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_SetItemContext ------------------------------------------------------------------

CypherParser::OC_SetItemContext::OC_SetItemContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_PropertyExpressionContext* CypherParser::OC_SetItemContext::oC_PropertyExpression() {
  return getRuleContext<CypherParser::OC_PropertyExpressionContext>(0);
}

CypherParser::OC_ExpressionContext* CypherParser::OC_SetItemContext::oC_Expression() {
  return getRuleContext<CypherParser::OC_ExpressionContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_SetItemContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_SetItemContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_SetItemContext::getRuleIndex() const {
  return CypherParser::RuleOC_SetItem;
}


CypherParser::OC_SetItemContext* CypherParser::oC_SetItem() {
  OC_SetItemContext *_localctx = _tracker.createInstance<OC_SetItemContext>(_ctx, getState());
  enterRule(_localctx, 78, CypherParser::RuleOC_SetItem);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(754);
    oC_PropertyExpression();
    setState(756);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(755);
      match(CypherParser::SP);
    }
    setState(758);
    match(CypherParser::T__6);
    setState(760);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(759);
      match(CypherParser::SP);
    }
    setState(762);
    oC_Expression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_DeleteContext ------------------------------------------------------------------

CypherParser::OC_DeleteContext::OC_DeleteContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_DeleteContext::DELETE() {
  return getToken(CypherParser::DELETE, 0);
}

std::vector<CypherParser::OC_ExpressionContext *> CypherParser::OC_DeleteContext::oC_Expression() {
  return getRuleContexts<CypherParser::OC_ExpressionContext>();
}

CypherParser::OC_ExpressionContext* CypherParser::OC_DeleteContext::oC_Expression(size_t i) {
  return getRuleContext<CypherParser::OC_ExpressionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_DeleteContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_DeleteContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_DeleteContext::getRuleIndex() const {
  return CypherParser::RuleOC_Delete;
}


CypherParser::OC_DeleteContext* CypherParser::oC_Delete() {
  OC_DeleteContext *_localctx = _tracker.createInstance<OC_DeleteContext>(_ctx, getState());
  enterRule(_localctx, 80, CypherParser::RuleOC_Delete);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(764);
    match(CypherParser::DELETE);
    setState(766);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(765);
      match(CypherParser::SP);
    }
    setState(768);
    oC_Expression();
    setState(779);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 101, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(770);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(769);
          match(CypherParser::SP);
        }
        setState(772);
        match(CypherParser::T__3);
        setState(774);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(773);
          match(CypherParser::SP);
        }
        setState(776);
        oC_Expression(); 
      }
      setState(781);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 101, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_WithContext ------------------------------------------------------------------

CypherParser::OC_WithContext::OC_WithContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_WithContext::WITH() {
  return getToken(CypherParser::WITH, 0);
}

CypherParser::OC_ProjectionBodyContext* CypherParser::OC_WithContext::oC_ProjectionBody() {
  return getRuleContext<CypherParser::OC_ProjectionBodyContext>(0);
}

CypherParser::OC_WhereContext* CypherParser::OC_WithContext::oC_Where() {
  return getRuleContext<CypherParser::OC_WhereContext>(0);
}

tree::TerminalNode* CypherParser::OC_WithContext::SP() {
  return getToken(CypherParser::SP, 0);
}


size_t CypherParser::OC_WithContext::getRuleIndex() const {
  return CypherParser::RuleOC_With;
}


CypherParser::OC_WithContext* CypherParser::oC_With() {
  OC_WithContext *_localctx = _tracker.createInstance<OC_WithContext>(_ctx, getState());
  enterRule(_localctx, 82, CypherParser::RuleOC_With);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(782);
    match(CypherParser::WITH);
    setState(783);
    oC_ProjectionBody();
    setState(788);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 103, _ctx)) {
    case 1: {
      setState(785);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(784);
        match(CypherParser::SP);
      }
      setState(787);
      oC_Where();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ReturnContext ------------------------------------------------------------------

CypherParser::OC_ReturnContext::OC_ReturnContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_ReturnContext::RETURN() {
  return getToken(CypherParser::RETURN, 0);
}

CypherParser::OC_ProjectionBodyContext* CypherParser::OC_ReturnContext::oC_ProjectionBody() {
  return getRuleContext<CypherParser::OC_ProjectionBodyContext>(0);
}


size_t CypherParser::OC_ReturnContext::getRuleIndex() const {
  return CypherParser::RuleOC_Return;
}


CypherParser::OC_ReturnContext* CypherParser::oC_Return() {
  OC_ReturnContext *_localctx = _tracker.createInstance<OC_ReturnContext>(_ctx, getState());
  enterRule(_localctx, 84, CypherParser::RuleOC_Return);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(790);
    match(CypherParser::RETURN);
    setState(791);
    oC_ProjectionBody();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ProjectionBodyContext ------------------------------------------------------------------

CypherParser::OC_ProjectionBodyContext::OC_ProjectionBodyContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<tree::TerminalNode *> CypherParser::OC_ProjectionBodyContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_ProjectionBodyContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OC_ProjectionItemsContext* CypherParser::OC_ProjectionBodyContext::oC_ProjectionItems() {
  return getRuleContext<CypherParser::OC_ProjectionItemsContext>(0);
}

tree::TerminalNode* CypherParser::OC_ProjectionBodyContext::DISTINCT() {
  return getToken(CypherParser::DISTINCT, 0);
}

CypherParser::OC_OrderContext* CypherParser::OC_ProjectionBodyContext::oC_Order() {
  return getRuleContext<CypherParser::OC_OrderContext>(0);
}

CypherParser::OC_SkipContext* CypherParser::OC_ProjectionBodyContext::oC_Skip() {
  return getRuleContext<CypherParser::OC_SkipContext>(0);
}

CypherParser::OC_LimitContext* CypherParser::OC_ProjectionBodyContext::oC_Limit() {
  return getRuleContext<CypherParser::OC_LimitContext>(0);
}


size_t CypherParser::OC_ProjectionBodyContext::getRuleIndex() const {
  return CypherParser::RuleOC_ProjectionBody;
}


CypherParser::OC_ProjectionBodyContext* CypherParser::oC_ProjectionBody() {
  OC_ProjectionBodyContext *_localctx = _tracker.createInstance<OC_ProjectionBodyContext>(_ctx, getState());
  enterRule(_localctx, 86, CypherParser::RuleOC_ProjectionBody);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(797);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 105, _ctx)) {
    case 1: {
      setState(794);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(793);
        match(CypherParser::SP);
      }
      setState(796);
      match(CypherParser::DISTINCT);
      break;
    }

    default:
      break;
    }
    setState(799);
    match(CypherParser::SP);
    setState(800);
    oC_ProjectionItems();
    setState(803);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 106, _ctx)) {
    case 1: {
      setState(801);
      match(CypherParser::SP);
      setState(802);
      oC_Order();
      break;
    }

    default:
      break;
    }
    setState(807);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 107, _ctx)) {
    case 1: {
      setState(805);
      match(CypherParser::SP);
      setState(806);
      oC_Skip();
      break;
    }

    default:
      break;
    }
    setState(811);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 108, _ctx)) {
    case 1: {
      setState(809);
      match(CypherParser::SP);
      setState(810);
      oC_Limit();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ProjectionItemsContext ------------------------------------------------------------------

CypherParser::OC_ProjectionItemsContext::OC_ProjectionItemsContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_ProjectionItemsContext::STAR() {
  return getToken(CypherParser::STAR, 0);
}

std::vector<CypherParser::OC_ProjectionItemContext *> CypherParser::OC_ProjectionItemsContext::oC_ProjectionItem() {
  return getRuleContexts<CypherParser::OC_ProjectionItemContext>();
}

CypherParser::OC_ProjectionItemContext* CypherParser::OC_ProjectionItemsContext::oC_ProjectionItem(size_t i) {
  return getRuleContext<CypherParser::OC_ProjectionItemContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_ProjectionItemsContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_ProjectionItemsContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_ProjectionItemsContext::getRuleIndex() const {
  return CypherParser::RuleOC_ProjectionItems;
}


CypherParser::OC_ProjectionItemsContext* CypherParser::oC_ProjectionItems() {
  OC_ProjectionItemsContext *_localctx = _tracker.createInstance<OC_ProjectionItemsContext>(_ctx, getState());
  enterRule(_localctx, 88, CypherParser::RuleOC_ProjectionItems);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    setState(841);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::STAR: {
        enterOuterAlt(_localctx, 1);
        setState(813);
        match(CypherParser::STAR);
        setState(824);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 111, _ctx);
        while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
          if (alt == 1) {
            setState(815);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(814);
              match(CypherParser::SP);
            }
            setState(817);
            match(CypherParser::T__3);
            setState(819);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(818);
              match(CypherParser::SP);
            }
            setState(821);
            oC_ProjectionItem(); 
          }
          setState(826);
          _errHandler->sync(this);
          alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 111, _ctx);
        }
        break;
      }

      case CypherParser::T__1:
      case CypherParser::T__4:
      case CypherParser::T__7:
      case CypherParser::T__26:
      case CypherParser::NOT:
      case CypherParser::MINUS:
      case CypherParser::NULL_:
      case CypherParser::TRUE:
      case CypherParser::FALSE:
      case CypherParser::EXISTS:
      case CypherParser::CASE:
      case CypherParser::StringLiteral:
      case CypherParser::DecimalInteger:
      case CypherParser::HexLetter:
      case CypherParser::RegularDecimalReal:
      case CypherParser::UnescapedSymbolicName:
      case CypherParser::EscapedSymbolicName: {
        enterOuterAlt(_localctx, 2);
        setState(827);
        oC_ProjectionItem();
        setState(838);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 114, _ctx);
        while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
          if (alt == 1) {
            setState(829);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(828);
              match(CypherParser::SP);
            }
            setState(831);
            match(CypherParser::T__3);
            setState(833);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(832);
              match(CypherParser::SP);
            }
            setState(835);
            oC_ProjectionItem(); 
          }
          setState(840);
          _errHandler->sync(this);
          alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 114, _ctx);
        }
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ProjectionItemContext ------------------------------------------------------------------

CypherParser::OC_ProjectionItemContext::OC_ProjectionItemContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_ExpressionContext* CypherParser::OC_ProjectionItemContext::oC_Expression() {
  return getRuleContext<CypherParser::OC_ExpressionContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_ProjectionItemContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_ProjectionItemContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::OC_ProjectionItemContext::AS() {
  return getToken(CypherParser::AS, 0);
}

CypherParser::OC_VariableContext* CypherParser::OC_ProjectionItemContext::oC_Variable() {
  return getRuleContext<CypherParser::OC_VariableContext>(0);
}


size_t CypherParser::OC_ProjectionItemContext::getRuleIndex() const {
  return CypherParser::RuleOC_ProjectionItem;
}


CypherParser::OC_ProjectionItemContext* CypherParser::oC_ProjectionItem() {
  OC_ProjectionItemContext *_localctx = _tracker.createInstance<OC_ProjectionItemContext>(_ctx, getState());
  enterRule(_localctx, 90, CypherParser::RuleOC_ProjectionItem);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(850);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 116, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(843);
      oC_Expression();
      setState(844);
      match(CypherParser::SP);
      setState(845);
      match(CypherParser::AS);
      setState(846);
      match(CypherParser::SP);
      setState(847);
      oC_Variable();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(849);
      oC_Expression();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_OrderContext ------------------------------------------------------------------

CypherParser::OC_OrderContext::OC_OrderContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_OrderContext::ORDER() {
  return getToken(CypherParser::ORDER, 0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_OrderContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_OrderContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::OC_OrderContext::BY() {
  return getToken(CypherParser::BY, 0);
}

std::vector<CypherParser::OC_SortItemContext *> CypherParser::OC_OrderContext::oC_SortItem() {
  return getRuleContexts<CypherParser::OC_SortItemContext>();
}

CypherParser::OC_SortItemContext* CypherParser::OC_OrderContext::oC_SortItem(size_t i) {
  return getRuleContext<CypherParser::OC_SortItemContext>(i);
}


size_t CypherParser::OC_OrderContext::getRuleIndex() const {
  return CypherParser::RuleOC_Order;
}


CypherParser::OC_OrderContext* CypherParser::oC_Order() {
  OC_OrderContext *_localctx = _tracker.createInstance<OC_OrderContext>(_ctx, getState());
  enterRule(_localctx, 92, CypherParser::RuleOC_Order);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(852);
    match(CypherParser::ORDER);
    setState(853);
    match(CypherParser::SP);
    setState(854);
    match(CypherParser::BY);
    setState(855);
    match(CypherParser::SP);
    setState(856);
    oC_SortItem();
    setState(864);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == CypherParser::T__3) {
      setState(857);
      match(CypherParser::T__3);
      setState(859);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(858);
        match(CypherParser::SP);
      }
      setState(861);
      oC_SortItem();
      setState(866);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_SkipContext ------------------------------------------------------------------

CypherParser::OC_SkipContext::OC_SkipContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_SkipContext::L_SKIP() {
  return getToken(CypherParser::L_SKIP, 0);
}

tree::TerminalNode* CypherParser::OC_SkipContext::SP() {
  return getToken(CypherParser::SP, 0);
}

CypherParser::OC_ExpressionContext* CypherParser::OC_SkipContext::oC_Expression() {
  return getRuleContext<CypherParser::OC_ExpressionContext>(0);
}


size_t CypherParser::OC_SkipContext::getRuleIndex() const {
  return CypherParser::RuleOC_Skip;
}


CypherParser::OC_SkipContext* CypherParser::oC_Skip() {
  OC_SkipContext *_localctx = _tracker.createInstance<OC_SkipContext>(_ctx, getState());
  enterRule(_localctx, 94, CypherParser::RuleOC_Skip);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(867);
    match(CypherParser::L_SKIP);
    setState(868);
    match(CypherParser::SP);
    setState(869);
    oC_Expression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_LimitContext ------------------------------------------------------------------

CypherParser::OC_LimitContext::OC_LimitContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_LimitContext::LIMIT() {
  return getToken(CypherParser::LIMIT, 0);
}

tree::TerminalNode* CypherParser::OC_LimitContext::SP() {
  return getToken(CypherParser::SP, 0);
}

CypherParser::OC_ExpressionContext* CypherParser::OC_LimitContext::oC_Expression() {
  return getRuleContext<CypherParser::OC_ExpressionContext>(0);
}


size_t CypherParser::OC_LimitContext::getRuleIndex() const {
  return CypherParser::RuleOC_Limit;
}


CypherParser::OC_LimitContext* CypherParser::oC_Limit() {
  OC_LimitContext *_localctx = _tracker.createInstance<OC_LimitContext>(_ctx, getState());
  enterRule(_localctx, 96, CypherParser::RuleOC_Limit);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(871);
    match(CypherParser::LIMIT);
    setState(872);
    match(CypherParser::SP);
    setState(873);
    oC_Expression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_SortItemContext ------------------------------------------------------------------

CypherParser::OC_SortItemContext::OC_SortItemContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_ExpressionContext* CypherParser::OC_SortItemContext::oC_Expression() {
  return getRuleContext<CypherParser::OC_ExpressionContext>(0);
}

tree::TerminalNode* CypherParser::OC_SortItemContext::ASCENDING() {
  return getToken(CypherParser::ASCENDING, 0);
}

tree::TerminalNode* CypherParser::OC_SortItemContext::ASC() {
  return getToken(CypherParser::ASC, 0);
}

tree::TerminalNode* CypherParser::OC_SortItemContext::DESCENDING() {
  return getToken(CypherParser::DESCENDING, 0);
}

tree::TerminalNode* CypherParser::OC_SortItemContext::DESC() {
  return getToken(CypherParser::DESC, 0);
}

tree::TerminalNode* CypherParser::OC_SortItemContext::SP() {
  return getToken(CypherParser::SP, 0);
}


size_t CypherParser::OC_SortItemContext::getRuleIndex() const {
  return CypherParser::RuleOC_SortItem;
}


CypherParser::OC_SortItemContext* CypherParser::oC_SortItem() {
  OC_SortItemContext *_localctx = _tracker.createInstance<OC_SortItemContext>(_ctx, getState());
  enterRule(_localctx, 98, CypherParser::RuleOC_SortItem);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(875);
    oC_Expression();
    setState(880);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 120, _ctx)) {
    case 1: {
      setState(877);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(876);
        match(CypherParser::SP);
      }
      setState(879);
      _la = _input->LA(1);
      if (!(((((_la - 82) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 82)) & ((1ULL << (CypherParser::ASCENDING - 82))
        | (1ULL << (CypherParser::ASC - 82))
        | (1ULL << (CypherParser::DESCENDING - 82))
        | (1ULL << (CypherParser::DESC - 82)))) != 0))) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_WhereContext ------------------------------------------------------------------

CypherParser::OC_WhereContext::OC_WhereContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_WhereContext::WHERE() {
  return getToken(CypherParser::WHERE, 0);
}

tree::TerminalNode* CypherParser::OC_WhereContext::SP() {
  return getToken(CypherParser::SP, 0);
}

CypherParser::OC_ExpressionContext* CypherParser::OC_WhereContext::oC_Expression() {
  return getRuleContext<CypherParser::OC_ExpressionContext>(0);
}


size_t CypherParser::OC_WhereContext::getRuleIndex() const {
  return CypherParser::RuleOC_Where;
}


CypherParser::OC_WhereContext* CypherParser::oC_Where() {
  OC_WhereContext *_localctx = _tracker.createInstance<OC_WhereContext>(_ctx, getState());
  enterRule(_localctx, 100, CypherParser::RuleOC_Where);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(882);
    match(CypherParser::WHERE);
    setState(883);
    match(CypherParser::SP);
    setState(884);
    oC_Expression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_PatternContext ------------------------------------------------------------------

CypherParser::OC_PatternContext::OC_PatternContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::OC_PatternPartContext *> CypherParser::OC_PatternContext::oC_PatternPart() {
  return getRuleContexts<CypherParser::OC_PatternPartContext>();
}

CypherParser::OC_PatternPartContext* CypherParser::OC_PatternContext::oC_PatternPart(size_t i) {
  return getRuleContext<CypherParser::OC_PatternPartContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_PatternContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_PatternContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_PatternContext::getRuleIndex() const {
  return CypherParser::RuleOC_Pattern;
}


CypherParser::OC_PatternContext* CypherParser::oC_Pattern() {
  OC_PatternContext *_localctx = _tracker.createInstance<OC_PatternContext>(_ctx, getState());
  enterRule(_localctx, 102, CypherParser::RuleOC_Pattern);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(886);
    oC_PatternPart();
    setState(897);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 123, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(888);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(887);
          match(CypherParser::SP);
        }
        setState(890);
        match(CypherParser::T__3);
        setState(892);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 122, _ctx)) {
        case 1: {
          setState(891);
          match(CypherParser::SP);
          break;
        }

        default:
          break;
        }
        setState(894);
        oC_PatternPart(); 
      }
      setState(899);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 123, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_PatternPartContext ------------------------------------------------------------------

CypherParser::OC_PatternPartContext::OC_PatternPartContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_AnonymousPatternPartContext* CypherParser::OC_PatternPartContext::oC_AnonymousPatternPart() {
  return getRuleContext<CypherParser::OC_AnonymousPatternPartContext>(0);
}


size_t CypherParser::OC_PatternPartContext::getRuleIndex() const {
  return CypherParser::RuleOC_PatternPart;
}


CypherParser::OC_PatternPartContext* CypherParser::oC_PatternPart() {
  OC_PatternPartContext *_localctx = _tracker.createInstance<OC_PatternPartContext>(_ctx, getState());
  enterRule(_localctx, 104, CypherParser::RuleOC_PatternPart);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(900);
    oC_AnonymousPatternPart();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_AnonymousPatternPartContext ------------------------------------------------------------------

CypherParser::OC_AnonymousPatternPartContext::OC_AnonymousPatternPartContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_PatternElementContext* CypherParser::OC_AnonymousPatternPartContext::oC_PatternElement() {
  return getRuleContext<CypherParser::OC_PatternElementContext>(0);
}


size_t CypherParser::OC_AnonymousPatternPartContext::getRuleIndex() const {
  return CypherParser::RuleOC_AnonymousPatternPart;
}


CypherParser::OC_AnonymousPatternPartContext* CypherParser::oC_AnonymousPatternPart() {
  OC_AnonymousPatternPartContext *_localctx = _tracker.createInstance<OC_AnonymousPatternPartContext>(_ctx, getState());
  enterRule(_localctx, 106, CypherParser::RuleOC_AnonymousPatternPart);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(902);
    oC_PatternElement();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_PatternElementContext ------------------------------------------------------------------

CypherParser::OC_PatternElementContext::OC_PatternElementContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_NodePatternContext* CypherParser::OC_PatternElementContext::oC_NodePattern() {
  return getRuleContext<CypherParser::OC_NodePatternContext>(0);
}

std::vector<CypherParser::OC_PatternElementChainContext *> CypherParser::OC_PatternElementContext::oC_PatternElementChain() {
  return getRuleContexts<CypherParser::OC_PatternElementChainContext>();
}

CypherParser::OC_PatternElementChainContext* CypherParser::OC_PatternElementContext::oC_PatternElementChain(size_t i) {
  return getRuleContext<CypherParser::OC_PatternElementChainContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_PatternElementContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_PatternElementContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OC_PatternElementContext* CypherParser::OC_PatternElementContext::oC_PatternElement() {
  return getRuleContext<CypherParser::OC_PatternElementContext>(0);
}


size_t CypherParser::OC_PatternElementContext::getRuleIndex() const {
  return CypherParser::RuleOC_PatternElement;
}


CypherParser::OC_PatternElementContext* CypherParser::oC_PatternElement() {
  OC_PatternElementContext *_localctx = _tracker.createInstance<OC_PatternElementContext>(_ctx, getState());
  enterRule(_localctx, 108, CypherParser::RuleOC_PatternElement);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    setState(918);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 126, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(904);
      oC_NodePattern();
      setState(911);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 125, _ctx);
      while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
        if (alt == 1) {
          setState(906);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == CypherParser::SP) {
            setState(905);
            match(CypherParser::SP);
          }
          setState(908);
          oC_PatternElementChain(); 
        }
        setState(913);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 125, _ctx);
      }
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(914);
      match(CypherParser::T__1);
      setState(915);
      oC_PatternElement();
      setState(916);
      match(CypherParser::T__2);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_NodePatternContext ------------------------------------------------------------------

CypherParser::OC_NodePatternContext::OC_NodePatternContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<tree::TerminalNode *> CypherParser::OC_NodePatternContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_NodePatternContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OC_VariableContext* CypherParser::OC_NodePatternContext::oC_Variable() {
  return getRuleContext<CypherParser::OC_VariableContext>(0);
}

CypherParser::OC_NodeLabelsContext* CypherParser::OC_NodePatternContext::oC_NodeLabels() {
  return getRuleContext<CypherParser::OC_NodeLabelsContext>(0);
}

CypherParser::KU_PropertiesContext* CypherParser::OC_NodePatternContext::kU_Properties() {
  return getRuleContext<CypherParser::KU_PropertiesContext>(0);
}


size_t CypherParser::OC_NodePatternContext::getRuleIndex() const {
  return CypherParser::RuleOC_NodePattern;
}


CypherParser::OC_NodePatternContext* CypherParser::oC_NodePattern() {
  OC_NodePatternContext *_localctx = _tracker.createInstance<OC_NodePatternContext>(_ctx, getState());
  enterRule(_localctx, 110, CypherParser::RuleOC_NodePattern);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(965);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::T__1: {
        enterOuterAlt(_localctx, 1);
        setState(920);
        match(CypherParser::T__1);
        setState(922);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(921);
          match(CypherParser::SP);
        }
        setState(928);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (((((_la - 111) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 111)) & ((1ULL << (CypherParser::HexLetter - 111))
          | (1ULL << (CypherParser::UnescapedSymbolicName - 111))
          | (1ULL << (CypherParser::EscapedSymbolicName - 111)))) != 0)) {
          setState(924);
          oC_Variable();
          setState(926);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == CypherParser::SP) {
            setState(925);
            match(CypherParser::SP);
          }
        }
        setState(934);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::T__8) {
          setState(930);
          oC_NodeLabels();
          setState(932);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == CypherParser::SP) {
            setState(931);
            match(CypherParser::SP);
          }
        }
        setState(940);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::T__7) {
          setState(936);
          kU_Properties();
          setState(938);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == CypherParser::SP) {
            setState(937);
            match(CypherParser::SP);
          }
        }
        setState(942);
        match(CypherParser::T__2);
        break;
      }

      case CypherParser::EOF:
      case CypherParser::T__0:
      case CypherParser::T__2:
      case CypherParser::T__3:
      case CypherParser::T__7:
      case CypherParser::T__8:
      case CypherParser::T__9:
      case CypherParser::T__13:
      case CypherParser::T__27:
      case CypherParser::T__28:
      case CypherParser::T__29:
      case CypherParser::T__30:
      case CypherParser::T__35:
      case CypherParser::T__36:
      case CypherParser::T__37:
      case CypherParser::T__38:
      case CypherParser::T__39:
      case CypherParser::T__40:
      case CypherParser::T__41:
      case CypherParser::T__42:
      case CypherParser::T__43:
      case CypherParser::T__44:
      case CypherParser::T__45:
      case CypherParser::UNION:
      case CypherParser::OPTIONAL:
      case CypherParser::MATCH:
      case CypherParser::UNWIND:
      case CypherParser::CREATE:
      case CypherParser::SET:
      case CypherParser::DELETE:
      case CypherParser::WITH:
      case CypherParser::RETURN:
      case CypherParser::WHERE:
      case CypherParser::MINUS:
      case CypherParser::HexLetter:
      case CypherParser::UnescapedSymbolicName:
      case CypherParser::EscapedSymbolicName:
      case CypherParser::SP: {
        enterOuterAlt(_localctx, 2);
        setState(944);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 134, _ctx)) {
        case 1: {
          setState(943);
          match(CypherParser::SP);
          break;
        }

        default:
          break;
        }
        setState(950);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (((((_la - 111) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 111)) & ((1ULL << (CypherParser::HexLetter - 111))
          | (1ULL << (CypherParser::UnescapedSymbolicName - 111))
          | (1ULL << (CypherParser::EscapedSymbolicName - 111)))) != 0)) {
          setState(946);
          dynamic_cast<OC_NodePatternContext *>(_localctx)->oC_VariableContext = oC_Variable();
          setState(948);
          _errHandler->sync(this);

          switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 135, _ctx)) {
          case 1: {
            setState(947);
            match(CypherParser::SP);
            break;
          }

          default:
            break;
          }
        }
        setState(956);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::T__8) {
          setState(952);
          oC_NodeLabels();
          setState(954);
          _errHandler->sync(this);

          switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 137, _ctx)) {
          case 1: {
            setState(953);
            match(CypherParser::SP);
            break;
          }

          default:
            break;
          }
        }
        setState(962);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::T__7) {
          setState(958);
          kU_Properties();
          setState(960);
          _errHandler->sync(this);

          switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 139, _ctx)) {
          case 1: {
            setState(959);
            match(CypherParser::SP);
            break;
          }

          default:
            break;
          }
        }
         notifyNodePatternWithoutParentheses((dynamic_cast<OC_NodePatternContext *>(_localctx)->oC_VariableContext != nullptr ? _input->getText(dynamic_cast<OC_NodePatternContext *>(_localctx)->oC_VariableContext->start, dynamic_cast<OC_NodePatternContext *>(_localctx)->oC_VariableContext->stop) : nullptr), (dynamic_cast<OC_NodePatternContext *>(_localctx)->oC_VariableContext != nullptr ? (dynamic_cast<OC_NodePatternContext *>(_localctx)->oC_VariableContext->start) : nullptr)); 
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_PatternElementChainContext ------------------------------------------------------------------

CypherParser::OC_PatternElementChainContext::OC_PatternElementChainContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_RelationshipPatternContext* CypherParser::OC_PatternElementChainContext::oC_RelationshipPattern() {
  return getRuleContext<CypherParser::OC_RelationshipPatternContext>(0);
}

CypherParser::OC_NodePatternContext* CypherParser::OC_PatternElementChainContext::oC_NodePattern() {
  return getRuleContext<CypherParser::OC_NodePatternContext>(0);
}

tree::TerminalNode* CypherParser::OC_PatternElementChainContext::SP() {
  return getToken(CypherParser::SP, 0);
}


size_t CypherParser::OC_PatternElementChainContext::getRuleIndex() const {
  return CypherParser::RuleOC_PatternElementChain;
}


CypherParser::OC_PatternElementChainContext* CypherParser::oC_PatternElementChain() {
  OC_PatternElementChainContext *_localctx = _tracker.createInstance<OC_PatternElementChainContext>(_ctx, getState());
  enterRule(_localctx, 112, CypherParser::RuleOC_PatternElementChain);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(967);
    oC_RelationshipPattern();
    setState(969);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 142, _ctx)) {
    case 1: {
      setState(968);
      match(CypherParser::SP);
      break;
    }

    default:
      break;
    }
    setState(971);
    oC_NodePattern();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_RelationshipPatternContext ------------------------------------------------------------------

CypherParser::OC_RelationshipPatternContext::OC_RelationshipPatternContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_LeftArrowHeadContext* CypherParser::OC_RelationshipPatternContext::oC_LeftArrowHead() {
  return getRuleContext<CypherParser::OC_LeftArrowHeadContext>(0);
}

std::vector<CypherParser::OC_DashContext *> CypherParser::OC_RelationshipPatternContext::oC_Dash() {
  return getRuleContexts<CypherParser::OC_DashContext>();
}

CypherParser::OC_DashContext* CypherParser::OC_RelationshipPatternContext::oC_Dash(size_t i) {
  return getRuleContext<CypherParser::OC_DashContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_RelationshipPatternContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_RelationshipPatternContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OC_RelationshipDetailContext* CypherParser::OC_RelationshipPatternContext::oC_RelationshipDetail() {
  return getRuleContext<CypherParser::OC_RelationshipDetailContext>(0);
}

CypherParser::OC_RightArrowHeadContext* CypherParser::OC_RelationshipPatternContext::oC_RightArrowHead() {
  return getRuleContext<CypherParser::OC_RightArrowHeadContext>(0);
}


size_t CypherParser::OC_RelationshipPatternContext::getRuleIndex() const {
  return CypherParser::RuleOC_RelationshipPattern;
}


CypherParser::OC_RelationshipPatternContext* CypherParser::oC_RelationshipPattern() {
  OC_RelationshipPatternContext *_localctx = _tracker.createInstance<OC_RelationshipPatternContext>(_ctx, getState());
  enterRule(_localctx, 114, CypherParser::RuleOC_RelationshipPattern);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1017);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 154, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(973);
      oC_LeftArrowHead();
      setState(975);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(974);
        match(CypherParser::SP);
      }
      setState(977);
      oC_Dash();
      setState(979);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 144, _ctx)) {
      case 1: {
        setState(978);
        match(CypherParser::SP);
        break;
      }

      default:
        break;
      }
      setState(982);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::T__4) {
        setState(981);
        oC_RelationshipDetail();
      }
      setState(985);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(984);
        match(CypherParser::SP);
      }
      setState(987);
      oC_Dash();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(989);
      oC_Dash();
      setState(991);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 147, _ctx)) {
      case 1: {
        setState(990);
        match(CypherParser::SP);
        break;
      }

      default:
        break;
      }
      setState(994);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::T__4) {
        setState(993);
        oC_RelationshipDetail();
      }
      setState(997);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(996);
        match(CypherParser::SP);
      }
      setState(999);
      oC_Dash();
      setState(1001);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1000);
        match(CypherParser::SP);
      }
      setState(1003);
      oC_RightArrowHead();
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(1005);
      oC_Dash();
      setState(1007);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 151, _ctx)) {
      case 1: {
        setState(1006);
        match(CypherParser::SP);
        break;
      }

      default:
        break;
      }
      setState(1010);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::T__4) {
        setState(1009);
        oC_RelationshipDetail();
      }
      setState(1013);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1012);
        match(CypherParser::SP);
      }
      setState(1015);
      oC_Dash();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_RelationshipDetailContext ------------------------------------------------------------------

CypherParser::OC_RelationshipDetailContext::OC_RelationshipDetailContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<tree::TerminalNode *> CypherParser::OC_RelationshipDetailContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_RelationshipDetailContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OC_VariableContext* CypherParser::OC_RelationshipDetailContext::oC_Variable() {
  return getRuleContext<CypherParser::OC_VariableContext>(0);
}

CypherParser::OC_RelationshipTypesContext* CypherParser::OC_RelationshipDetailContext::oC_RelationshipTypes() {
  return getRuleContext<CypherParser::OC_RelationshipTypesContext>(0);
}

CypherParser::OC_RangeLiteralContext* CypherParser::OC_RelationshipDetailContext::oC_RangeLiteral() {
  return getRuleContext<CypherParser::OC_RangeLiteralContext>(0);
}

CypherParser::KU_PropertiesContext* CypherParser::OC_RelationshipDetailContext::kU_Properties() {
  return getRuleContext<CypherParser::KU_PropertiesContext>(0);
}


size_t CypherParser::OC_RelationshipDetailContext::getRuleIndex() const {
  return CypherParser::RuleOC_RelationshipDetail;
}


CypherParser::OC_RelationshipDetailContext* CypherParser::oC_RelationshipDetail() {
  OC_RelationshipDetailContext *_localctx = _tracker.createInstance<OC_RelationshipDetailContext>(_ctx, getState());
  enterRule(_localctx, 116, CypherParser::RuleOC_RelationshipDetail);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1019);
    match(CypherParser::T__4);
    setState(1021);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1020);
      match(CypherParser::SP);
    }
    setState(1027);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (((((_la - 111) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 111)) & ((1ULL << (CypherParser::HexLetter - 111))
      | (1ULL << (CypherParser::UnescapedSymbolicName - 111))
      | (1ULL << (CypherParser::EscapedSymbolicName - 111)))) != 0)) {
      setState(1023);
      oC_Variable();
      setState(1025);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1024);
        match(CypherParser::SP);
      }
    }
    setState(1033);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::T__8) {
      setState(1029);
      oC_RelationshipTypes();
      setState(1031);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1030);
        match(CypherParser::SP);
      }
    }
    setState(1039);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::STAR) {
      setState(1035);
      oC_RangeLiteral();
      setState(1037);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1036);
        match(CypherParser::SP);
      }
    }
    setState(1045);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::T__7) {
      setState(1041);
      kU_Properties();
      setState(1043);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1042);
        match(CypherParser::SP);
      }
    }
    setState(1047);
    match(CypherParser::T__5);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_PropertiesContext ------------------------------------------------------------------

CypherParser::KU_PropertiesContext::KU_PropertiesContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<tree::TerminalNode *> CypherParser::KU_PropertiesContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_PropertiesContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<CypherParser::OC_PropertyKeyNameContext *> CypherParser::KU_PropertiesContext::oC_PropertyKeyName() {
  return getRuleContexts<CypherParser::OC_PropertyKeyNameContext>();
}

CypherParser::OC_PropertyKeyNameContext* CypherParser::KU_PropertiesContext::oC_PropertyKeyName(size_t i) {
  return getRuleContext<CypherParser::OC_PropertyKeyNameContext>(i);
}

std::vector<CypherParser::OC_ExpressionContext *> CypherParser::KU_PropertiesContext::oC_Expression() {
  return getRuleContexts<CypherParser::OC_ExpressionContext>();
}

CypherParser::OC_ExpressionContext* CypherParser::KU_PropertiesContext::oC_Expression(size_t i) {
  return getRuleContext<CypherParser::OC_ExpressionContext>(i);
}


size_t CypherParser::KU_PropertiesContext::getRuleIndex() const {
  return CypherParser::RuleKU_Properties;
}


CypherParser::KU_PropertiesContext* CypherParser::kU_Properties() {
  KU_PropertiesContext *_localctx = _tracker.createInstance<KU_PropertiesContext>(_ctx, getState());
  enterRule(_localctx, 118, CypherParser::RuleKU_Properties);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1049);
    match(CypherParser::T__7);
    setState(1051);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1050);
      match(CypherParser::SP);
    }
    setState(1086);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (((((_la - 111) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 111)) & ((1ULL << (CypherParser::HexLetter - 111))
      | (1ULL << (CypherParser::UnescapedSymbolicName - 111))
      | (1ULL << (CypherParser::EscapedSymbolicName - 111)))) != 0)) {
      setState(1053);
      oC_PropertyKeyName();
      setState(1055);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1054);
        match(CypherParser::SP);
      }
      setState(1057);
      match(CypherParser::T__8);
      setState(1059);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1058);
        match(CypherParser::SP);
      }
      setState(1061);
      oC_Expression();
      setState(1063);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1062);
        match(CypherParser::SP);
      }
      setState(1083);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == CypherParser::T__3) {
        setState(1065);
        match(CypherParser::T__3);
        setState(1067);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1066);
          match(CypherParser::SP);
        }
        setState(1069);
        oC_PropertyKeyName();
        setState(1071);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1070);
          match(CypherParser::SP);
        }
        setState(1073);
        match(CypherParser::T__8);
        setState(1075);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1074);
          match(CypherParser::SP);
        }
        setState(1077);
        oC_Expression();
        setState(1079);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1078);
          match(CypherParser::SP);
        }
        setState(1085);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(1088);
    match(CypherParser::T__9);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_RelationshipTypesContext ------------------------------------------------------------------

CypherParser::OC_RelationshipTypesContext::OC_RelationshipTypesContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::OC_RelTypeNameContext *> CypherParser::OC_RelationshipTypesContext::oC_RelTypeName() {
  return getRuleContexts<CypherParser::OC_RelTypeNameContext>();
}

CypherParser::OC_RelTypeNameContext* CypherParser::OC_RelationshipTypesContext::oC_RelTypeName(size_t i) {
  return getRuleContext<CypherParser::OC_RelTypeNameContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_RelationshipTypesContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_RelationshipTypesContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_RelationshipTypesContext::getRuleIndex() const {
  return CypherParser::RuleOC_RelationshipTypes;
}


CypherParser::OC_RelationshipTypesContext* CypherParser::oC_RelationshipTypes() {
  OC_RelationshipTypesContext *_localctx = _tracker.createInstance<OC_RelationshipTypesContext>(_ctx, getState());
  enterRule(_localctx, 120, CypherParser::RuleOC_RelationshipTypes);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1090);
    match(CypherParser::T__8);
    setState(1092);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1091);
      match(CypherParser::SP);
    }
    setState(1094);
    oC_RelTypeName();
    setState(1108);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 178, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1096);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1095);
          match(CypherParser::SP);
        }
        setState(1098);
        match(CypherParser::T__10);
        setState(1100);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::T__8) {
          setState(1099);
          match(CypherParser::T__8);
        }
        setState(1103);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1102);
          match(CypherParser::SP);
        }
        setState(1105);
        oC_RelTypeName(); 
      }
      setState(1110);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 178, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_NodeLabelsContext ------------------------------------------------------------------

CypherParser::OC_NodeLabelsContext::OC_NodeLabelsContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::OC_NodeLabelContext *> CypherParser::OC_NodeLabelsContext::oC_NodeLabel() {
  return getRuleContexts<CypherParser::OC_NodeLabelContext>();
}

CypherParser::OC_NodeLabelContext* CypherParser::OC_NodeLabelsContext::oC_NodeLabel(size_t i) {
  return getRuleContext<CypherParser::OC_NodeLabelContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_NodeLabelsContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_NodeLabelsContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_NodeLabelsContext::getRuleIndex() const {
  return CypherParser::RuleOC_NodeLabels;
}


CypherParser::OC_NodeLabelsContext* CypherParser::oC_NodeLabels() {
  OC_NodeLabelsContext *_localctx = _tracker.createInstance<OC_NodeLabelsContext>(_ctx, getState());
  enterRule(_localctx, 122, CypherParser::RuleOC_NodeLabels);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1111);
    oC_NodeLabel();
    setState(1118);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 180, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1113);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1112);
          match(CypherParser::SP);
        }
        setState(1115);
        oC_NodeLabel(); 
      }
      setState(1120);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 180, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_NodeLabelContext ------------------------------------------------------------------

CypherParser::OC_NodeLabelContext::OC_NodeLabelContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_LabelNameContext* CypherParser::OC_NodeLabelContext::oC_LabelName() {
  return getRuleContext<CypherParser::OC_LabelNameContext>(0);
}

tree::TerminalNode* CypherParser::OC_NodeLabelContext::SP() {
  return getToken(CypherParser::SP, 0);
}


size_t CypherParser::OC_NodeLabelContext::getRuleIndex() const {
  return CypherParser::RuleOC_NodeLabel;
}


CypherParser::OC_NodeLabelContext* CypherParser::oC_NodeLabel() {
  OC_NodeLabelContext *_localctx = _tracker.createInstance<OC_NodeLabelContext>(_ctx, getState());
  enterRule(_localctx, 124, CypherParser::RuleOC_NodeLabel);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1121);
    match(CypherParser::T__8);
    setState(1123);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1122);
      match(CypherParser::SP);
    }
    setState(1125);
    oC_LabelName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_RangeLiteralContext ------------------------------------------------------------------

CypherParser::OC_RangeLiteralContext::OC_RangeLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_RangeLiteralContext::STAR() {
  return getToken(CypherParser::STAR, 0);
}

std::vector<CypherParser::OC_IntegerLiteralContext *> CypherParser::OC_RangeLiteralContext::oC_IntegerLiteral() {
  return getRuleContexts<CypherParser::OC_IntegerLiteralContext>();
}

CypherParser::OC_IntegerLiteralContext* CypherParser::OC_RangeLiteralContext::oC_IntegerLiteral(size_t i) {
  return getRuleContext<CypherParser::OC_IntegerLiteralContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_RangeLiteralContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_RangeLiteralContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::OC_RangeLiteralContext::SHORTEST() {
  return getToken(CypherParser::SHORTEST, 0);
}


size_t CypherParser::OC_RangeLiteralContext::getRuleIndex() const {
  return CypherParser::RuleOC_RangeLiteral;
}


CypherParser::OC_RangeLiteralContext* CypherParser::oC_RangeLiteral() {
  OC_RangeLiteralContext *_localctx = _tracker.createInstance<OC_RangeLiteralContext>(_ctx, getState());
  enterRule(_localctx, 126, CypherParser::RuleOC_RangeLiteral);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1127);
    match(CypherParser::STAR);
    setState(1129);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 182, _ctx)) {
    case 1: {
      setState(1128);
      match(CypherParser::SP);
      break;
    }

    default:
      break;
    }
    setState(1132);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SHORTEST) {
      setState(1131);
      match(CypherParser::SHORTEST);
    }
    setState(1135);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1134);
      match(CypherParser::SP);
    }
    setState(1137);
    oC_IntegerLiteral();
    setState(1139);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1138);
      match(CypherParser::SP);
    }
    setState(1141);
    match(CypherParser::T__11);
    setState(1143);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1142);
      match(CypherParser::SP);
    }
    setState(1145);
    oC_IntegerLiteral();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_LabelNameContext ------------------------------------------------------------------

CypherParser::OC_LabelNameContext::OC_LabelNameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SchemaNameContext* CypherParser::OC_LabelNameContext::oC_SchemaName() {
  return getRuleContext<CypherParser::OC_SchemaNameContext>(0);
}


size_t CypherParser::OC_LabelNameContext::getRuleIndex() const {
  return CypherParser::RuleOC_LabelName;
}


CypherParser::OC_LabelNameContext* CypherParser::oC_LabelName() {
  OC_LabelNameContext *_localctx = _tracker.createInstance<OC_LabelNameContext>(_ctx, getState());
  enterRule(_localctx, 128, CypherParser::RuleOC_LabelName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1147);
    oC_SchemaName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_RelTypeNameContext ------------------------------------------------------------------

CypherParser::OC_RelTypeNameContext::OC_RelTypeNameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SchemaNameContext* CypherParser::OC_RelTypeNameContext::oC_SchemaName() {
  return getRuleContext<CypherParser::OC_SchemaNameContext>(0);
}


size_t CypherParser::OC_RelTypeNameContext::getRuleIndex() const {
  return CypherParser::RuleOC_RelTypeName;
}


CypherParser::OC_RelTypeNameContext* CypherParser::oC_RelTypeName() {
  OC_RelTypeNameContext *_localctx = _tracker.createInstance<OC_RelTypeNameContext>(_ctx, getState());
  enterRule(_localctx, 130, CypherParser::RuleOC_RelTypeName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1149);
    oC_SchemaName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ExpressionContext ------------------------------------------------------------------

CypherParser::OC_ExpressionContext::OC_ExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_OrExpressionContext* CypherParser::OC_ExpressionContext::oC_OrExpression() {
  return getRuleContext<CypherParser::OC_OrExpressionContext>(0);
}


size_t CypherParser::OC_ExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_Expression;
}


CypherParser::OC_ExpressionContext* CypherParser::oC_Expression() {
  OC_ExpressionContext *_localctx = _tracker.createInstance<OC_ExpressionContext>(_ctx, getState());
  enterRule(_localctx, 132, CypherParser::RuleOC_Expression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1151);
    oC_OrExpression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_OrExpressionContext ------------------------------------------------------------------

CypherParser::OC_OrExpressionContext::OC_OrExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::OC_XorExpressionContext *> CypherParser::OC_OrExpressionContext::oC_XorExpression() {
  return getRuleContexts<CypherParser::OC_XorExpressionContext>();
}

CypherParser::OC_XorExpressionContext* CypherParser::OC_OrExpressionContext::oC_XorExpression(size_t i) {
  return getRuleContext<CypherParser::OC_XorExpressionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_OrExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_OrExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_OrExpressionContext::OR() {
  return getTokens(CypherParser::OR);
}

tree::TerminalNode* CypherParser::OC_OrExpressionContext::OR(size_t i) {
  return getToken(CypherParser::OR, i);
}


size_t CypherParser::OC_OrExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_OrExpression;
}


CypherParser::OC_OrExpressionContext* CypherParser::oC_OrExpression() {
  OC_OrExpressionContext *_localctx = _tracker.createInstance<OC_OrExpressionContext>(_ctx, getState());
  enterRule(_localctx, 134, CypherParser::RuleOC_OrExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1153);
    oC_XorExpression();
    setState(1160);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 187, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1154);
        match(CypherParser::SP);
        setState(1155);
        match(CypherParser::OR);
        setState(1156);
        match(CypherParser::SP);
        setState(1157);
        oC_XorExpression(); 
      }
      setState(1162);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 187, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_XorExpressionContext ------------------------------------------------------------------

CypherParser::OC_XorExpressionContext::OC_XorExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::OC_AndExpressionContext *> CypherParser::OC_XorExpressionContext::oC_AndExpression() {
  return getRuleContexts<CypherParser::OC_AndExpressionContext>();
}

CypherParser::OC_AndExpressionContext* CypherParser::OC_XorExpressionContext::oC_AndExpression(size_t i) {
  return getRuleContext<CypherParser::OC_AndExpressionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_XorExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_XorExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_XorExpressionContext::XOR() {
  return getTokens(CypherParser::XOR);
}

tree::TerminalNode* CypherParser::OC_XorExpressionContext::XOR(size_t i) {
  return getToken(CypherParser::XOR, i);
}


size_t CypherParser::OC_XorExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_XorExpression;
}


CypherParser::OC_XorExpressionContext* CypherParser::oC_XorExpression() {
  OC_XorExpressionContext *_localctx = _tracker.createInstance<OC_XorExpressionContext>(_ctx, getState());
  enterRule(_localctx, 136, CypherParser::RuleOC_XorExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1163);
    oC_AndExpression();
    setState(1170);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 188, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1164);
        match(CypherParser::SP);
        setState(1165);
        match(CypherParser::XOR);
        setState(1166);
        match(CypherParser::SP);
        setState(1167);
        oC_AndExpression(); 
      }
      setState(1172);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 188, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_AndExpressionContext ------------------------------------------------------------------

CypherParser::OC_AndExpressionContext::OC_AndExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::OC_NotExpressionContext *> CypherParser::OC_AndExpressionContext::oC_NotExpression() {
  return getRuleContexts<CypherParser::OC_NotExpressionContext>();
}

CypherParser::OC_NotExpressionContext* CypherParser::OC_AndExpressionContext::oC_NotExpression(size_t i) {
  return getRuleContext<CypherParser::OC_NotExpressionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_AndExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_AndExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_AndExpressionContext::AND() {
  return getTokens(CypherParser::AND);
}

tree::TerminalNode* CypherParser::OC_AndExpressionContext::AND(size_t i) {
  return getToken(CypherParser::AND, i);
}


size_t CypherParser::OC_AndExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_AndExpression;
}


CypherParser::OC_AndExpressionContext* CypherParser::oC_AndExpression() {
  OC_AndExpressionContext *_localctx = _tracker.createInstance<OC_AndExpressionContext>(_ctx, getState());
  enterRule(_localctx, 138, CypherParser::RuleOC_AndExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1173);
    oC_NotExpression();
    setState(1180);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 189, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1174);
        match(CypherParser::SP);
        setState(1175);
        match(CypherParser::AND);
        setState(1176);
        match(CypherParser::SP);
        setState(1177);
        oC_NotExpression(); 
      }
      setState(1182);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 189, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_NotExpressionContext ------------------------------------------------------------------

CypherParser::OC_NotExpressionContext::OC_NotExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_ComparisonExpressionContext* CypherParser::OC_NotExpressionContext::oC_ComparisonExpression() {
  return getRuleContext<CypherParser::OC_ComparisonExpressionContext>(0);
}

tree::TerminalNode* CypherParser::OC_NotExpressionContext::NOT() {
  return getToken(CypherParser::NOT, 0);
}

tree::TerminalNode* CypherParser::OC_NotExpressionContext::SP() {
  return getToken(CypherParser::SP, 0);
}


size_t CypherParser::OC_NotExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_NotExpression;
}


CypherParser::OC_NotExpressionContext* CypherParser::oC_NotExpression() {
  OC_NotExpressionContext *_localctx = _tracker.createInstance<OC_NotExpressionContext>(_ctx, getState());
  enterRule(_localctx, 140, CypherParser::RuleOC_NotExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1187);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::NOT) {
      setState(1183);
      match(CypherParser::NOT);
      setState(1185);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1184);
        match(CypherParser::SP);
      }
    }
    setState(1189);
    oC_ComparisonExpression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ComparisonExpressionContext ------------------------------------------------------------------

CypherParser::OC_ComparisonExpressionContext::OC_ComparisonExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::KU_BitwiseOrOperatorExpressionContext *> CypherParser::OC_ComparisonExpressionContext::kU_BitwiseOrOperatorExpression() {
  return getRuleContexts<CypherParser::KU_BitwiseOrOperatorExpressionContext>();
}

CypherParser::KU_BitwiseOrOperatorExpressionContext* CypherParser::OC_ComparisonExpressionContext::kU_BitwiseOrOperatorExpression(size_t i) {
  return getRuleContext<CypherParser::KU_BitwiseOrOperatorExpressionContext>(i);
}

std::vector<CypherParser::KU_ComparisonOperatorContext *> CypherParser::OC_ComparisonExpressionContext::kU_ComparisonOperator() {
  return getRuleContexts<CypherParser::KU_ComparisonOperatorContext>();
}

CypherParser::KU_ComparisonOperatorContext* CypherParser::OC_ComparisonExpressionContext::kU_ComparisonOperator(size_t i) {
  return getRuleContext<CypherParser::KU_ComparisonOperatorContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_ComparisonExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_ComparisonExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::OC_ComparisonExpressionContext::INVALID_NOT_EQUAL() {
  return getToken(CypherParser::INVALID_NOT_EQUAL, 0);
}


size_t CypherParser::OC_ComparisonExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_ComparisonExpression;
}


CypherParser::OC_ComparisonExpressionContext* CypherParser::oC_ComparisonExpression() {
  OC_ComparisonExpressionContext *_localctx = _tracker.createInstance<OC_ComparisonExpressionContext>(_ctx, getState());
  enterRule(_localctx, 142, CypherParser::RuleOC_ComparisonExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    setState(1239);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 202, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(1191);
      kU_BitwiseOrOperatorExpression();
      setState(1201);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 194, _ctx)) {
      case 1: {
        setState(1193);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1192);
          match(CypherParser::SP);
        }
        setState(1195);
        kU_ComparisonOperator();
        setState(1197);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1196);
          match(CypherParser::SP);
        }
        setState(1199);
        kU_BitwiseOrOperatorExpression();
        break;
      }

      default:
        break;
      }
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(1203);
      kU_BitwiseOrOperatorExpression();

      setState(1205);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1204);
        match(CypherParser::SP);
      }
      setState(1207);
      dynamic_cast<OC_ComparisonExpressionContext *>(_localctx)->invalid_not_equalToken = match(CypherParser::INVALID_NOT_EQUAL);
      setState(1209);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1208);
        match(CypherParser::SP);
      }
      setState(1211);
      kU_BitwiseOrOperatorExpression();
       notifyInvalidNotEqualOperator(dynamic_cast<OC_ComparisonExpressionContext *>(_localctx)->invalid_not_equalToken); 
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(1215);
      kU_BitwiseOrOperatorExpression();
      setState(1217);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1216);
        match(CypherParser::SP);
      }
      setState(1219);
      kU_ComparisonOperator();
      setState(1221);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1220);
        match(CypherParser::SP);
      }
      setState(1223);
      kU_BitwiseOrOperatorExpression();
      setState(1233); 
      _errHandler->sync(this);
      alt = 1;
      do {
        switch (alt) {
          case 1: {
                setState(1225);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == CypherParser::SP) {
                  setState(1224);
                  match(CypherParser::SP);
                }
                setState(1227);
                kU_ComparisonOperator();
                setState(1229);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == CypherParser::SP) {
                  setState(1228);
                  match(CypherParser::SP);
                }
                setState(1231);
                kU_BitwiseOrOperatorExpression();
                break;
              }

        default:
          throw NoViableAltException(this);
        }
        setState(1235); 
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 201, _ctx);
      } while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER);
       notifyNonBinaryComparison(_localctx->start); 
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_ComparisonOperatorContext ------------------------------------------------------------------

CypherParser::KU_ComparisonOperatorContext::KU_ComparisonOperatorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t CypherParser::KU_ComparisonOperatorContext::getRuleIndex() const {
  return CypherParser::RuleKU_ComparisonOperator;
}


CypherParser::KU_ComparisonOperatorContext* CypherParser::kU_ComparisonOperator() {
  KU_ComparisonOperatorContext *_localctx = _tracker.createInstance<KU_ComparisonOperatorContext>(_ctx, getState());
  enterRule(_localctx, 144, CypherParser::RuleKU_ComparisonOperator);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1241);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << CypherParser::T__6)
      | (1ULL << CypherParser::T__12)
      | (1ULL << CypherParser::T__13)
      | (1ULL << CypherParser::T__14)
      | (1ULL << CypherParser::T__15)
      | (1ULL << CypherParser::T__16))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_BitwiseOrOperatorExpressionContext ------------------------------------------------------------------

CypherParser::KU_BitwiseOrOperatorExpressionContext::KU_BitwiseOrOperatorExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::KU_BitwiseAndOperatorExpressionContext *> CypherParser::KU_BitwiseOrOperatorExpressionContext::kU_BitwiseAndOperatorExpression() {
  return getRuleContexts<CypherParser::KU_BitwiseAndOperatorExpressionContext>();
}

CypherParser::KU_BitwiseAndOperatorExpressionContext* CypherParser::KU_BitwiseOrOperatorExpressionContext::kU_BitwiseAndOperatorExpression(size_t i) {
  return getRuleContext<CypherParser::KU_BitwiseAndOperatorExpressionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::KU_BitwiseOrOperatorExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_BitwiseOrOperatorExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::KU_BitwiseOrOperatorExpressionContext::getRuleIndex() const {
  return CypherParser::RuleKU_BitwiseOrOperatorExpression;
}


CypherParser::KU_BitwiseOrOperatorExpressionContext* CypherParser::kU_BitwiseOrOperatorExpression() {
  KU_BitwiseOrOperatorExpressionContext *_localctx = _tracker.createInstance<KU_BitwiseOrOperatorExpressionContext>(_ctx, getState());
  enterRule(_localctx, 146, CypherParser::RuleKU_BitwiseOrOperatorExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1243);
    kU_BitwiseAndOperatorExpression();
    setState(1254);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 205, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1245);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1244);
          match(CypherParser::SP);
        }
        setState(1247);
        match(CypherParser::T__10);
        setState(1249);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1248);
          match(CypherParser::SP);
        }
        setState(1251);
        kU_BitwiseAndOperatorExpression(); 
      }
      setState(1256);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 205, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_BitwiseAndOperatorExpressionContext ------------------------------------------------------------------

CypherParser::KU_BitwiseAndOperatorExpressionContext::KU_BitwiseAndOperatorExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::KU_BitShiftOperatorExpressionContext *> CypherParser::KU_BitwiseAndOperatorExpressionContext::kU_BitShiftOperatorExpression() {
  return getRuleContexts<CypherParser::KU_BitShiftOperatorExpressionContext>();
}

CypherParser::KU_BitShiftOperatorExpressionContext* CypherParser::KU_BitwiseAndOperatorExpressionContext::kU_BitShiftOperatorExpression(size_t i) {
  return getRuleContext<CypherParser::KU_BitShiftOperatorExpressionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::KU_BitwiseAndOperatorExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_BitwiseAndOperatorExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::KU_BitwiseAndOperatorExpressionContext::getRuleIndex() const {
  return CypherParser::RuleKU_BitwiseAndOperatorExpression;
}


CypherParser::KU_BitwiseAndOperatorExpressionContext* CypherParser::kU_BitwiseAndOperatorExpression() {
  KU_BitwiseAndOperatorExpressionContext *_localctx = _tracker.createInstance<KU_BitwiseAndOperatorExpressionContext>(_ctx, getState());
  enterRule(_localctx, 148, CypherParser::RuleKU_BitwiseAndOperatorExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1257);
    kU_BitShiftOperatorExpression();
    setState(1268);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 208, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1259);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1258);
          match(CypherParser::SP);
        }
        setState(1261);
        match(CypherParser::T__17);
        setState(1263);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1262);
          match(CypherParser::SP);
        }
        setState(1265);
        kU_BitShiftOperatorExpression(); 
      }
      setState(1270);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 208, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_BitShiftOperatorExpressionContext ------------------------------------------------------------------

CypherParser::KU_BitShiftOperatorExpressionContext::KU_BitShiftOperatorExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::OC_AddOrSubtractExpressionContext *> CypherParser::KU_BitShiftOperatorExpressionContext::oC_AddOrSubtractExpression() {
  return getRuleContexts<CypherParser::OC_AddOrSubtractExpressionContext>();
}

CypherParser::OC_AddOrSubtractExpressionContext* CypherParser::KU_BitShiftOperatorExpressionContext::oC_AddOrSubtractExpression(size_t i) {
  return getRuleContext<CypherParser::OC_AddOrSubtractExpressionContext>(i);
}

std::vector<CypherParser::KU_BitShiftOperatorContext *> CypherParser::KU_BitShiftOperatorExpressionContext::kU_BitShiftOperator() {
  return getRuleContexts<CypherParser::KU_BitShiftOperatorContext>();
}

CypherParser::KU_BitShiftOperatorContext* CypherParser::KU_BitShiftOperatorExpressionContext::kU_BitShiftOperator(size_t i) {
  return getRuleContext<CypherParser::KU_BitShiftOperatorContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::KU_BitShiftOperatorExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_BitShiftOperatorExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::KU_BitShiftOperatorExpressionContext::getRuleIndex() const {
  return CypherParser::RuleKU_BitShiftOperatorExpression;
}


CypherParser::KU_BitShiftOperatorExpressionContext* CypherParser::kU_BitShiftOperatorExpression() {
  KU_BitShiftOperatorExpressionContext *_localctx = _tracker.createInstance<KU_BitShiftOperatorExpressionContext>(_ctx, getState());
  enterRule(_localctx, 150, CypherParser::RuleKU_BitShiftOperatorExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1271);
    oC_AddOrSubtractExpression();
    setState(1283);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 211, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1273);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1272);
          match(CypherParser::SP);
        }
        setState(1275);
        kU_BitShiftOperator();
        setState(1277);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1276);
          match(CypherParser::SP);
        }
        setState(1279);
        oC_AddOrSubtractExpression(); 
      }
      setState(1285);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 211, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_BitShiftOperatorContext ------------------------------------------------------------------

CypherParser::KU_BitShiftOperatorContext::KU_BitShiftOperatorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t CypherParser::KU_BitShiftOperatorContext::getRuleIndex() const {
  return CypherParser::RuleKU_BitShiftOperator;
}


CypherParser::KU_BitShiftOperatorContext* CypherParser::kU_BitShiftOperator() {
  KU_BitShiftOperatorContext *_localctx = _tracker.createInstance<KU_BitShiftOperatorContext>(_ctx, getState());
  enterRule(_localctx, 152, CypherParser::RuleKU_BitShiftOperator);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1286);
    _la = _input->LA(1);
    if (!(_la == CypherParser::T__18

    || _la == CypherParser::T__19)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_AddOrSubtractExpressionContext ------------------------------------------------------------------

CypherParser::OC_AddOrSubtractExpressionContext::OC_AddOrSubtractExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::OC_MultiplyDivideModuloExpressionContext *> CypherParser::OC_AddOrSubtractExpressionContext::oC_MultiplyDivideModuloExpression() {
  return getRuleContexts<CypherParser::OC_MultiplyDivideModuloExpressionContext>();
}

CypherParser::OC_MultiplyDivideModuloExpressionContext* CypherParser::OC_AddOrSubtractExpressionContext::oC_MultiplyDivideModuloExpression(size_t i) {
  return getRuleContext<CypherParser::OC_MultiplyDivideModuloExpressionContext>(i);
}

std::vector<CypherParser::KU_AddOrSubtractOperatorContext *> CypherParser::OC_AddOrSubtractExpressionContext::kU_AddOrSubtractOperator() {
  return getRuleContexts<CypherParser::KU_AddOrSubtractOperatorContext>();
}

CypherParser::KU_AddOrSubtractOperatorContext* CypherParser::OC_AddOrSubtractExpressionContext::kU_AddOrSubtractOperator(size_t i) {
  return getRuleContext<CypherParser::KU_AddOrSubtractOperatorContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_AddOrSubtractExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_AddOrSubtractExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_AddOrSubtractExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_AddOrSubtractExpression;
}


CypherParser::OC_AddOrSubtractExpressionContext* CypherParser::oC_AddOrSubtractExpression() {
  OC_AddOrSubtractExpressionContext *_localctx = _tracker.createInstance<OC_AddOrSubtractExpressionContext>(_ctx, getState());
  enterRule(_localctx, 154, CypherParser::RuleOC_AddOrSubtractExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1288);
    oC_MultiplyDivideModuloExpression();
    setState(1300);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 214, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1290);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1289);
          match(CypherParser::SP);
        }
        setState(1292);
        kU_AddOrSubtractOperator();
        setState(1294);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1293);
          match(CypherParser::SP);
        }
        setState(1296);
        oC_MultiplyDivideModuloExpression(); 
      }
      setState(1302);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 214, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_AddOrSubtractOperatorContext ------------------------------------------------------------------

CypherParser::KU_AddOrSubtractOperatorContext::KU_AddOrSubtractOperatorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_AddOrSubtractOperatorContext::MINUS() {
  return getToken(CypherParser::MINUS, 0);
}


size_t CypherParser::KU_AddOrSubtractOperatorContext::getRuleIndex() const {
  return CypherParser::RuleKU_AddOrSubtractOperator;
}


CypherParser::KU_AddOrSubtractOperatorContext* CypherParser::kU_AddOrSubtractOperator() {
  KU_AddOrSubtractOperatorContext *_localctx = _tracker.createInstance<KU_AddOrSubtractOperatorContext>(_ctx, getState());
  enterRule(_localctx, 156, CypherParser::RuleKU_AddOrSubtractOperator);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1303);
    _la = _input->LA(1);
    if (!(_la == CypherParser::T__20 || _la == CypherParser::MINUS)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_MultiplyDivideModuloExpressionContext ------------------------------------------------------------------

CypherParser::OC_MultiplyDivideModuloExpressionContext::OC_MultiplyDivideModuloExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::OC_PowerOfExpressionContext *> CypherParser::OC_MultiplyDivideModuloExpressionContext::oC_PowerOfExpression() {
  return getRuleContexts<CypherParser::OC_PowerOfExpressionContext>();
}

CypherParser::OC_PowerOfExpressionContext* CypherParser::OC_MultiplyDivideModuloExpressionContext::oC_PowerOfExpression(size_t i) {
  return getRuleContext<CypherParser::OC_PowerOfExpressionContext>(i);
}

std::vector<CypherParser::KU_MultiplyDivideModuloOperatorContext *> CypherParser::OC_MultiplyDivideModuloExpressionContext::kU_MultiplyDivideModuloOperator() {
  return getRuleContexts<CypherParser::KU_MultiplyDivideModuloOperatorContext>();
}

CypherParser::KU_MultiplyDivideModuloOperatorContext* CypherParser::OC_MultiplyDivideModuloExpressionContext::kU_MultiplyDivideModuloOperator(size_t i) {
  return getRuleContext<CypherParser::KU_MultiplyDivideModuloOperatorContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_MultiplyDivideModuloExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_MultiplyDivideModuloExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_MultiplyDivideModuloExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_MultiplyDivideModuloExpression;
}


CypherParser::OC_MultiplyDivideModuloExpressionContext* CypherParser::oC_MultiplyDivideModuloExpression() {
  OC_MultiplyDivideModuloExpressionContext *_localctx = _tracker.createInstance<OC_MultiplyDivideModuloExpressionContext>(_ctx, getState());
  enterRule(_localctx, 158, CypherParser::RuleOC_MultiplyDivideModuloExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1305);
    oC_PowerOfExpression();
    setState(1317);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 217, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1307);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1306);
          match(CypherParser::SP);
        }
        setState(1309);
        kU_MultiplyDivideModuloOperator();
        setState(1311);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1310);
          match(CypherParser::SP);
        }
        setState(1313);
        oC_PowerOfExpression(); 
      }
      setState(1319);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 217, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_MultiplyDivideModuloOperatorContext ------------------------------------------------------------------

CypherParser::KU_MultiplyDivideModuloOperatorContext::KU_MultiplyDivideModuloOperatorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_MultiplyDivideModuloOperatorContext::STAR() {
  return getToken(CypherParser::STAR, 0);
}


size_t CypherParser::KU_MultiplyDivideModuloOperatorContext::getRuleIndex() const {
  return CypherParser::RuleKU_MultiplyDivideModuloOperator;
}


CypherParser::KU_MultiplyDivideModuloOperatorContext* CypherParser::kU_MultiplyDivideModuloOperator() {
  KU_MultiplyDivideModuloOperatorContext *_localctx = _tracker.createInstance<KU_MultiplyDivideModuloOperatorContext>(_ctx, getState());
  enterRule(_localctx, 160, CypherParser::RuleKU_MultiplyDivideModuloOperator);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1320);
    _la = _input->LA(1);
    if (!(((((_la - 22) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 22)) & ((1ULL << (CypherParser::T__21 - 22))
      | (1ULL << (CypherParser::T__22 - 22))
      | (1ULL << (CypherParser::STAR - 22)))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_PowerOfExpressionContext ------------------------------------------------------------------

CypherParser::OC_PowerOfExpressionContext::OC_PowerOfExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext *> CypherParser::OC_PowerOfExpressionContext::oC_UnaryAddSubtractOrFactorialExpression() {
  return getRuleContexts<CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext>();
}

CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext* CypherParser::OC_PowerOfExpressionContext::oC_UnaryAddSubtractOrFactorialExpression(size_t i) {
  return getRuleContext<CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_PowerOfExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_PowerOfExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_PowerOfExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_PowerOfExpression;
}


CypherParser::OC_PowerOfExpressionContext* CypherParser::oC_PowerOfExpression() {
  OC_PowerOfExpressionContext *_localctx = _tracker.createInstance<OC_PowerOfExpressionContext>(_ctx, getState());
  enterRule(_localctx, 162, CypherParser::RuleOC_PowerOfExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1322);
    oC_UnaryAddSubtractOrFactorialExpression();
    setState(1333);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 220, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1324);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1323);
          match(CypherParser::SP);
        }
        setState(1326);
        match(CypherParser::T__23);
        setState(1328);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1327);
          match(CypherParser::SP);
        }
        setState(1330);
        oC_UnaryAddSubtractOrFactorialExpression(); 
      }
      setState(1335);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 220, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_UnaryAddSubtractOrFactorialExpressionContext ------------------------------------------------------------------

CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext::OC_UnaryAddSubtractOrFactorialExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_StringListNullOperatorExpressionContext* CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext::oC_StringListNullOperatorExpression() {
  return getRuleContext<CypherParser::OC_StringListNullOperatorExpressionContext>(0);
}

tree::TerminalNode* CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext::MINUS() {
  return getToken(CypherParser::MINUS, 0);
}

tree::TerminalNode* CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext::FACTORIAL() {
  return getToken(CypherParser::FACTORIAL, 0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_UnaryAddSubtractOrFactorialExpression;
}


CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext* CypherParser::oC_UnaryAddSubtractOrFactorialExpression() {
  OC_UnaryAddSubtractOrFactorialExpressionContext *_localctx = _tracker.createInstance<OC_UnaryAddSubtractOrFactorialExpressionContext>(_ctx, getState());
  enterRule(_localctx, 164, CypherParser::RuleOC_UnaryAddSubtractOrFactorialExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1340);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::MINUS) {
      setState(1336);
      match(CypherParser::MINUS);
      setState(1338);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1337);
        match(CypherParser::SP);
      }
    }
    setState(1342);
    oC_StringListNullOperatorExpression();
    setState(1347);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 224, _ctx)) {
    case 1: {
      setState(1344);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1343);
        match(CypherParser::SP);
      }
      setState(1346);
      match(CypherParser::FACTORIAL);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_StringListNullOperatorExpressionContext ------------------------------------------------------------------

CypherParser::OC_StringListNullOperatorExpressionContext::OC_StringListNullOperatorExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_PropertyOrLabelsExpressionContext* CypherParser::OC_StringListNullOperatorExpressionContext::oC_PropertyOrLabelsExpression() {
  return getRuleContext<CypherParser::OC_PropertyOrLabelsExpressionContext>(0);
}

CypherParser::OC_StringOperatorExpressionContext* CypherParser::OC_StringListNullOperatorExpressionContext::oC_StringOperatorExpression() {
  return getRuleContext<CypherParser::OC_StringOperatorExpressionContext>(0);
}

CypherParser::OC_ListOperatorExpressionContext* CypherParser::OC_StringListNullOperatorExpressionContext::oC_ListOperatorExpression() {
  return getRuleContext<CypherParser::OC_ListOperatorExpressionContext>(0);
}

CypherParser::OC_NullOperatorExpressionContext* CypherParser::OC_StringListNullOperatorExpressionContext::oC_NullOperatorExpression() {
  return getRuleContext<CypherParser::OC_NullOperatorExpressionContext>(0);
}


size_t CypherParser::OC_StringListNullOperatorExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_StringListNullOperatorExpression;
}


CypherParser::OC_StringListNullOperatorExpressionContext* CypherParser::oC_StringListNullOperatorExpression() {
  OC_StringListNullOperatorExpressionContext *_localctx = _tracker.createInstance<OC_StringListNullOperatorExpressionContext>(_ctx, getState());
  enterRule(_localctx, 166, CypherParser::RuleOC_StringListNullOperatorExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1349);
    oC_PropertyOrLabelsExpression();
    setState(1353);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 225, _ctx)) {
    case 1: {
      setState(1350);
      oC_StringOperatorExpression();
      break;
    }

    case 2: {
      setState(1351);
      oC_ListOperatorExpression();
      break;
    }

    case 3: {
      setState(1352);
      oC_NullOperatorExpression();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ListOperatorExpressionContext ------------------------------------------------------------------

CypherParser::OC_ListOperatorExpressionContext::OC_ListOperatorExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::KU_ListExtractOperatorExpressionContext* CypherParser::OC_ListOperatorExpressionContext::kU_ListExtractOperatorExpression() {
  return getRuleContext<CypherParser::KU_ListExtractOperatorExpressionContext>(0);
}

CypherParser::KU_ListSliceOperatorExpressionContext* CypherParser::OC_ListOperatorExpressionContext::kU_ListSliceOperatorExpression() {
  return getRuleContext<CypherParser::KU_ListSliceOperatorExpressionContext>(0);
}

CypherParser::OC_ListOperatorExpressionContext* CypherParser::OC_ListOperatorExpressionContext::oC_ListOperatorExpression() {
  return getRuleContext<CypherParser::OC_ListOperatorExpressionContext>(0);
}


size_t CypherParser::OC_ListOperatorExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_ListOperatorExpression;
}


CypherParser::OC_ListOperatorExpressionContext* CypherParser::oC_ListOperatorExpression() {
  OC_ListOperatorExpressionContext *_localctx = _tracker.createInstance<OC_ListOperatorExpressionContext>(_ctx, getState());
  enterRule(_localctx, 168, CypherParser::RuleOC_ListOperatorExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1357);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 226, _ctx)) {
    case 1: {
      setState(1355);
      kU_ListExtractOperatorExpression();
      break;
    }

    case 2: {
      setState(1356);
      kU_ListSliceOperatorExpression();
      break;
    }

    default:
      break;
    }
    setState(1360);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 227, _ctx)) {
    case 1: {
      setState(1359);
      oC_ListOperatorExpression();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_ListExtractOperatorExpressionContext ------------------------------------------------------------------

CypherParser::KU_ListExtractOperatorExpressionContext::KU_ListExtractOperatorExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_ExpressionContext* CypherParser::KU_ListExtractOperatorExpressionContext::oC_Expression() {
  return getRuleContext<CypherParser::OC_ExpressionContext>(0);
}

tree::TerminalNode* CypherParser::KU_ListExtractOperatorExpressionContext::SP() {
  return getToken(CypherParser::SP, 0);
}


size_t CypherParser::KU_ListExtractOperatorExpressionContext::getRuleIndex() const {
  return CypherParser::RuleKU_ListExtractOperatorExpression;
}


CypherParser::KU_ListExtractOperatorExpressionContext* CypherParser::kU_ListExtractOperatorExpression() {
  KU_ListExtractOperatorExpressionContext *_localctx = _tracker.createInstance<KU_ListExtractOperatorExpressionContext>(_ctx, getState());
  enterRule(_localctx, 170, CypherParser::RuleKU_ListExtractOperatorExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1363);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1362);
      match(CypherParser::SP);
    }
    setState(1365);
    match(CypherParser::T__4);
    setState(1366);
    oC_Expression();
    setState(1367);
    match(CypherParser::T__5);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_ListSliceOperatorExpressionContext ------------------------------------------------------------------

CypherParser::KU_ListSliceOperatorExpressionContext::KU_ListSliceOperatorExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_ListSliceOperatorExpressionContext::SP() {
  return getToken(CypherParser::SP, 0);
}

std::vector<CypherParser::OC_ExpressionContext *> CypherParser::KU_ListSliceOperatorExpressionContext::oC_Expression() {
  return getRuleContexts<CypherParser::OC_ExpressionContext>();
}

CypherParser::OC_ExpressionContext* CypherParser::KU_ListSliceOperatorExpressionContext::oC_Expression(size_t i) {
  return getRuleContext<CypherParser::OC_ExpressionContext>(i);
}


size_t CypherParser::KU_ListSliceOperatorExpressionContext::getRuleIndex() const {
  return CypherParser::RuleKU_ListSliceOperatorExpression;
}


CypherParser::KU_ListSliceOperatorExpressionContext* CypherParser::kU_ListSliceOperatorExpression() {
  KU_ListSliceOperatorExpressionContext *_localctx = _tracker.createInstance<KU_ListSliceOperatorExpressionContext>(_ctx, getState());
  enterRule(_localctx, 172, CypherParser::RuleKU_ListSliceOperatorExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1370);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1369);
      match(CypherParser::SP);
    }
    setState(1372);
    match(CypherParser::T__4);
    setState(1374);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << CypherParser::T__1)
      | (1ULL << CypherParser::T__4)
      | (1ULL << CypherParser::T__7)
      | (1ULL << CypherParser::T__26))) != 0) || ((((_la - 91) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 91)) & ((1ULL << (CypherParser::NOT - 91))
      | (1ULL << (CypherParser::MINUS - 91))
      | (1ULL << (CypherParser::NULL_ - 91))
      | (1ULL << (CypherParser::TRUE - 91))
      | (1ULL << (CypherParser::FALSE - 91))
      | (1ULL << (CypherParser::EXISTS - 91))
      | (1ULL << (CypherParser::CASE - 91))
      | (1ULL << (CypherParser::StringLiteral - 91))
      | (1ULL << (CypherParser::DecimalInteger - 91))
      | (1ULL << (CypherParser::HexLetter - 91))
      | (1ULL << (CypherParser::RegularDecimalReal - 91))
      | (1ULL << (CypherParser::UnescapedSymbolicName - 91))
      | (1ULL << (CypherParser::EscapedSymbolicName - 91)))) != 0)) {
      setState(1373);
      oC_Expression();
    }
    setState(1376);
    match(CypherParser::T__8);
    setState(1378);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << CypherParser::T__1)
      | (1ULL << CypherParser::T__4)
      | (1ULL << CypherParser::T__7)
      | (1ULL << CypherParser::T__26))) != 0) || ((((_la - 91) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 91)) & ((1ULL << (CypherParser::NOT - 91))
      | (1ULL << (CypherParser::MINUS - 91))
      | (1ULL << (CypherParser::NULL_ - 91))
      | (1ULL << (CypherParser::TRUE - 91))
      | (1ULL << (CypherParser::FALSE - 91))
      | (1ULL << (CypherParser::EXISTS - 91))
      | (1ULL << (CypherParser::CASE - 91))
      | (1ULL << (CypherParser::StringLiteral - 91))
      | (1ULL << (CypherParser::DecimalInteger - 91))
      | (1ULL << (CypherParser::HexLetter - 91))
      | (1ULL << (CypherParser::RegularDecimalReal - 91))
      | (1ULL << (CypherParser::UnescapedSymbolicName - 91))
      | (1ULL << (CypherParser::EscapedSymbolicName - 91)))) != 0)) {
      setState(1377);
      oC_Expression();
    }
    setState(1380);
    match(CypherParser::T__5);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_StringOperatorExpressionContext ------------------------------------------------------------------

CypherParser::OC_StringOperatorExpressionContext::OC_StringOperatorExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_PropertyOrLabelsExpressionContext* CypherParser::OC_StringOperatorExpressionContext::oC_PropertyOrLabelsExpression() {
  return getRuleContext<CypherParser::OC_PropertyOrLabelsExpressionContext>(0);
}

CypherParser::OC_RegularExpressionContext* CypherParser::OC_StringOperatorExpressionContext::oC_RegularExpression() {
  return getRuleContext<CypherParser::OC_RegularExpressionContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_StringOperatorExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_StringOperatorExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::OC_StringOperatorExpressionContext::STARTS() {
  return getToken(CypherParser::STARTS, 0);
}

tree::TerminalNode* CypherParser::OC_StringOperatorExpressionContext::WITH() {
  return getToken(CypherParser::WITH, 0);
}

tree::TerminalNode* CypherParser::OC_StringOperatorExpressionContext::ENDS() {
  return getToken(CypherParser::ENDS, 0);
}

tree::TerminalNode* CypherParser::OC_StringOperatorExpressionContext::CONTAINS() {
  return getToken(CypherParser::CONTAINS, 0);
}


size_t CypherParser::OC_StringOperatorExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_StringOperatorExpression;
}


CypherParser::OC_StringOperatorExpressionContext* CypherParser::oC_StringOperatorExpression() {
  OC_StringOperatorExpressionContext *_localctx = _tracker.createInstance<OC_StringOperatorExpressionContext>(_ctx, getState());
  enterRule(_localctx, 174, CypherParser::RuleOC_StringOperatorExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1393);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 232, _ctx)) {
    case 1: {
      setState(1382);
      oC_RegularExpression();
      break;
    }

    case 2: {
      setState(1383);
      match(CypherParser::SP);
      setState(1384);
      match(CypherParser::STARTS);
      setState(1385);
      match(CypherParser::SP);
      setState(1386);
      match(CypherParser::WITH);
      break;
    }

    case 3: {
      setState(1387);
      match(CypherParser::SP);
      setState(1388);
      match(CypherParser::ENDS);
      setState(1389);
      match(CypherParser::SP);
      setState(1390);
      match(CypherParser::WITH);
      break;
    }

    case 4: {
      setState(1391);
      match(CypherParser::SP);
      setState(1392);
      match(CypherParser::CONTAINS);
      break;
    }

    default:
      break;
    }
    setState(1396);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1395);
      match(CypherParser::SP);
    }
    setState(1398);
    oC_PropertyOrLabelsExpression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_RegularExpressionContext ------------------------------------------------------------------

CypherParser::OC_RegularExpressionContext::OC_RegularExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_RegularExpressionContext::SP() {
  return getToken(CypherParser::SP, 0);
}


size_t CypherParser::OC_RegularExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_RegularExpression;
}


CypherParser::OC_RegularExpressionContext* CypherParser::oC_RegularExpression() {
  OC_RegularExpressionContext *_localctx = _tracker.createInstance<OC_RegularExpressionContext>(_ctx, getState());
  enterRule(_localctx, 176, CypherParser::RuleOC_RegularExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1401);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1400);
      match(CypherParser::SP);
    }
    setState(1403);
    match(CypherParser::T__24);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_NullOperatorExpressionContext ------------------------------------------------------------------

CypherParser::OC_NullOperatorExpressionContext::OC_NullOperatorExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<tree::TerminalNode *> CypherParser::OC_NullOperatorExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_NullOperatorExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::OC_NullOperatorExpressionContext::IS() {
  return getToken(CypherParser::IS, 0);
}

tree::TerminalNode* CypherParser::OC_NullOperatorExpressionContext::NULL_() {
  return getToken(CypherParser::NULL_, 0);
}

tree::TerminalNode* CypherParser::OC_NullOperatorExpressionContext::NOT() {
  return getToken(CypherParser::NOT, 0);
}


size_t CypherParser::OC_NullOperatorExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_NullOperatorExpression;
}


CypherParser::OC_NullOperatorExpressionContext* CypherParser::oC_NullOperatorExpression() {
  OC_NullOperatorExpressionContext *_localctx = _tracker.createInstance<OC_NullOperatorExpressionContext>(_ctx, getState());
  enterRule(_localctx, 178, CypherParser::RuleOC_NullOperatorExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1415);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 235, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(1405);
      match(CypherParser::SP);
      setState(1406);
      match(CypherParser::IS);
      setState(1407);
      match(CypherParser::SP);
      setState(1408);
      match(CypherParser::NULL_);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(1409);
      match(CypherParser::SP);
      setState(1410);
      match(CypherParser::IS);
      setState(1411);
      match(CypherParser::SP);
      setState(1412);
      match(CypherParser::NOT);
      setState(1413);
      match(CypherParser::SP);
      setState(1414);
      match(CypherParser::NULL_);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_PropertyOrLabelsExpressionContext ------------------------------------------------------------------

CypherParser::OC_PropertyOrLabelsExpressionContext::OC_PropertyOrLabelsExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_AtomContext* CypherParser::OC_PropertyOrLabelsExpressionContext::oC_Atom() {
  return getRuleContext<CypherParser::OC_AtomContext>(0);
}

CypherParser::OC_PropertyLookupContext* CypherParser::OC_PropertyOrLabelsExpressionContext::oC_PropertyLookup() {
  return getRuleContext<CypherParser::OC_PropertyLookupContext>(0);
}

tree::TerminalNode* CypherParser::OC_PropertyOrLabelsExpressionContext::SP() {
  return getToken(CypherParser::SP, 0);
}


size_t CypherParser::OC_PropertyOrLabelsExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_PropertyOrLabelsExpression;
}


CypherParser::OC_PropertyOrLabelsExpressionContext* CypherParser::oC_PropertyOrLabelsExpression() {
  OC_PropertyOrLabelsExpressionContext *_localctx = _tracker.createInstance<OC_PropertyOrLabelsExpressionContext>(_ctx, getState());
  enterRule(_localctx, 180, CypherParser::RuleOC_PropertyOrLabelsExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1417);
    oC_Atom();
    setState(1422);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 237, _ctx)) {
    case 1: {
      setState(1419);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1418);
        match(CypherParser::SP);
      }
      setState(1421);
      oC_PropertyLookup();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_AtomContext ------------------------------------------------------------------

CypherParser::OC_AtomContext::OC_AtomContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_LiteralContext* CypherParser::OC_AtomContext::oC_Literal() {
  return getRuleContext<CypherParser::OC_LiteralContext>(0);
}

CypherParser::OC_ParameterContext* CypherParser::OC_AtomContext::oC_Parameter() {
  return getRuleContext<CypherParser::OC_ParameterContext>(0);
}

CypherParser::OC_CaseExpressionContext* CypherParser::OC_AtomContext::oC_CaseExpression() {
  return getRuleContext<CypherParser::OC_CaseExpressionContext>(0);
}

CypherParser::OC_ParenthesizedExpressionContext* CypherParser::OC_AtomContext::oC_ParenthesizedExpression() {
  return getRuleContext<CypherParser::OC_ParenthesizedExpressionContext>(0);
}

CypherParser::OC_FunctionInvocationContext* CypherParser::OC_AtomContext::oC_FunctionInvocation() {
  return getRuleContext<CypherParser::OC_FunctionInvocationContext>(0);
}

CypherParser::OC_ExistentialSubqueryContext* CypherParser::OC_AtomContext::oC_ExistentialSubquery() {
  return getRuleContext<CypherParser::OC_ExistentialSubqueryContext>(0);
}

CypherParser::OC_VariableContext* CypherParser::OC_AtomContext::oC_Variable() {
  return getRuleContext<CypherParser::OC_VariableContext>(0);
}


size_t CypherParser::OC_AtomContext::getRuleIndex() const {
  return CypherParser::RuleOC_Atom;
}


CypherParser::OC_AtomContext* CypherParser::oC_Atom() {
  OC_AtomContext *_localctx = _tracker.createInstance<OC_AtomContext>(_ctx, getState());
  enterRule(_localctx, 182, CypherParser::RuleOC_Atom);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1431);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 238, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(1424);
      oC_Literal();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(1425);
      oC_Parameter();
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(1426);
      oC_CaseExpression();
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(1427);
      oC_ParenthesizedExpression();
      break;
    }

    case 5: {
      enterOuterAlt(_localctx, 5);
      setState(1428);
      oC_FunctionInvocation();
      break;
    }

    case 6: {
      enterOuterAlt(_localctx, 6);
      setState(1429);
      oC_ExistentialSubquery();
      break;
    }

    case 7: {
      enterOuterAlt(_localctx, 7);
      setState(1430);
      oC_Variable();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_LiteralContext ------------------------------------------------------------------

CypherParser::OC_LiteralContext::OC_LiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_NumberLiteralContext* CypherParser::OC_LiteralContext::oC_NumberLiteral() {
  return getRuleContext<CypherParser::OC_NumberLiteralContext>(0);
}

tree::TerminalNode* CypherParser::OC_LiteralContext::StringLiteral() {
  return getToken(CypherParser::StringLiteral, 0);
}

CypherParser::OC_BooleanLiteralContext* CypherParser::OC_LiteralContext::oC_BooleanLiteral() {
  return getRuleContext<CypherParser::OC_BooleanLiteralContext>(0);
}

tree::TerminalNode* CypherParser::OC_LiteralContext::NULL_() {
  return getToken(CypherParser::NULL_, 0);
}

CypherParser::OC_ListLiteralContext* CypherParser::OC_LiteralContext::oC_ListLiteral() {
  return getRuleContext<CypherParser::OC_ListLiteralContext>(0);
}

CypherParser::KU_StructLiteralContext* CypherParser::OC_LiteralContext::kU_StructLiteral() {
  return getRuleContext<CypherParser::KU_StructLiteralContext>(0);
}


size_t CypherParser::OC_LiteralContext::getRuleIndex() const {
  return CypherParser::RuleOC_Literal;
}


CypherParser::OC_LiteralContext* CypherParser::oC_Literal() {
  OC_LiteralContext *_localctx = _tracker.createInstance<OC_LiteralContext>(_ctx, getState());
  enterRule(_localctx, 184, CypherParser::RuleOC_Literal);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1439);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::DecimalInteger:
      case CypherParser::RegularDecimalReal: {
        enterOuterAlt(_localctx, 1);
        setState(1433);
        oC_NumberLiteral();
        break;
      }

      case CypherParser::StringLiteral: {
        enterOuterAlt(_localctx, 2);
        setState(1434);
        match(CypherParser::StringLiteral);
        break;
      }

      case CypherParser::TRUE:
      case CypherParser::FALSE: {
        enterOuterAlt(_localctx, 3);
        setState(1435);
        oC_BooleanLiteral();
        break;
      }

      case CypherParser::NULL_: {
        enterOuterAlt(_localctx, 4);
        setState(1436);
        match(CypherParser::NULL_);
        break;
      }

      case CypherParser::T__4: {
        enterOuterAlt(_localctx, 5);
        setState(1437);
        oC_ListLiteral();
        break;
      }

      case CypherParser::T__7: {
        enterOuterAlt(_localctx, 6);
        setState(1438);
        kU_StructLiteral();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_BooleanLiteralContext ------------------------------------------------------------------

CypherParser::OC_BooleanLiteralContext::OC_BooleanLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_BooleanLiteralContext::TRUE() {
  return getToken(CypherParser::TRUE, 0);
}

tree::TerminalNode* CypherParser::OC_BooleanLiteralContext::FALSE() {
  return getToken(CypherParser::FALSE, 0);
}


size_t CypherParser::OC_BooleanLiteralContext::getRuleIndex() const {
  return CypherParser::RuleOC_BooleanLiteral;
}


CypherParser::OC_BooleanLiteralContext* CypherParser::oC_BooleanLiteral() {
  OC_BooleanLiteralContext *_localctx = _tracker.createInstance<OC_BooleanLiteralContext>(_ctx, getState());
  enterRule(_localctx, 186, CypherParser::RuleOC_BooleanLiteral);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1441);
    _la = _input->LA(1);
    if (!(_la == CypherParser::TRUE

    || _la == CypherParser::FALSE)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ListLiteralContext ------------------------------------------------------------------

CypherParser::OC_ListLiteralContext::OC_ListLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<tree::TerminalNode *> CypherParser::OC_ListLiteralContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_ListLiteralContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<CypherParser::OC_ExpressionContext *> CypherParser::OC_ListLiteralContext::oC_Expression() {
  return getRuleContexts<CypherParser::OC_ExpressionContext>();
}

CypherParser::OC_ExpressionContext* CypherParser::OC_ListLiteralContext::oC_Expression(size_t i) {
  return getRuleContext<CypherParser::OC_ExpressionContext>(i);
}


size_t CypherParser::OC_ListLiteralContext::getRuleIndex() const {
  return CypherParser::RuleOC_ListLiteral;
}


CypherParser::OC_ListLiteralContext* CypherParser::oC_ListLiteral() {
  OC_ListLiteralContext *_localctx = _tracker.createInstance<OC_ListLiteralContext>(_ctx, getState());
  enterRule(_localctx, 188, CypherParser::RuleOC_ListLiteral);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1443);
    match(CypherParser::T__4);
    setState(1445);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1444);
      match(CypherParser::SP);
    }
    setState(1464);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << CypherParser::T__1)
      | (1ULL << CypherParser::T__4)
      | (1ULL << CypherParser::T__7)
      | (1ULL << CypherParser::T__26))) != 0) || ((((_la - 91) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 91)) & ((1ULL << (CypherParser::NOT - 91))
      | (1ULL << (CypherParser::MINUS - 91))
      | (1ULL << (CypherParser::NULL_ - 91))
      | (1ULL << (CypherParser::TRUE - 91))
      | (1ULL << (CypherParser::FALSE - 91))
      | (1ULL << (CypherParser::EXISTS - 91))
      | (1ULL << (CypherParser::CASE - 91))
      | (1ULL << (CypherParser::StringLiteral - 91))
      | (1ULL << (CypherParser::DecimalInteger - 91))
      | (1ULL << (CypherParser::HexLetter - 91))
      | (1ULL << (CypherParser::RegularDecimalReal - 91))
      | (1ULL << (CypherParser::UnescapedSymbolicName - 91))
      | (1ULL << (CypherParser::EscapedSymbolicName - 91)))) != 0)) {
      setState(1447);
      oC_Expression();
      setState(1449);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1448);
        match(CypherParser::SP);
      }
      setState(1461);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == CypherParser::T__3) {
        setState(1451);
        match(CypherParser::T__3);
        setState(1453);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1452);
          match(CypherParser::SP);
        }
        setState(1455);
        oC_Expression();
        setState(1457);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1456);
          match(CypherParser::SP);
        }
        setState(1463);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(1466);
    match(CypherParser::T__5);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_StructLiteralContext ------------------------------------------------------------------

CypherParser::KU_StructLiteralContext::KU_StructLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::KU_StructFieldContext *> CypherParser::KU_StructLiteralContext::kU_StructField() {
  return getRuleContexts<CypherParser::KU_StructFieldContext>();
}

CypherParser::KU_StructFieldContext* CypherParser::KU_StructLiteralContext::kU_StructField(size_t i) {
  return getRuleContext<CypherParser::KU_StructFieldContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::KU_StructLiteralContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_StructLiteralContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::KU_StructLiteralContext::getRuleIndex() const {
  return CypherParser::RuleKU_StructLiteral;
}


CypherParser::KU_StructLiteralContext* CypherParser::kU_StructLiteral() {
  KU_StructLiteralContext *_localctx = _tracker.createInstance<KU_StructLiteralContext>(_ctx, getState());
  enterRule(_localctx, 190, CypherParser::RuleKU_StructLiteral);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1468);
    match(CypherParser::T__7);
    setState(1470);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1469);
      match(CypherParser::SP);
    }
    setState(1472);
    kU_StructField();
    setState(1474);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1473);
      match(CypherParser::SP);
    }
    setState(1486);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == CypherParser::T__3) {
      setState(1476);
      match(CypherParser::T__3);
      setState(1478);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1477);
        match(CypherParser::SP);
      }
      setState(1480);
      kU_StructField();
      setState(1482);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1481);
        match(CypherParser::SP);
      }
      setState(1488);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(1489);
    match(CypherParser::T__9);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_StructFieldContext ------------------------------------------------------------------

CypherParser::KU_StructFieldContext::KU_StructFieldContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SymbolicNameContext* CypherParser::KU_StructFieldContext::oC_SymbolicName() {
  return getRuleContext<CypherParser::OC_SymbolicNameContext>(0);
}

CypherParser::OC_ExpressionContext* CypherParser::KU_StructFieldContext::oC_Expression() {
  return getRuleContext<CypherParser::OC_ExpressionContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_StructFieldContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_StructFieldContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::KU_StructFieldContext::getRuleIndex() const {
  return CypherParser::RuleKU_StructField;
}


CypherParser::KU_StructFieldContext* CypherParser::kU_StructField() {
  KU_StructFieldContext *_localctx = _tracker.createInstance<KU_StructFieldContext>(_ctx, getState());
  enterRule(_localctx, 192, CypherParser::RuleKU_StructField);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1491);
    oC_SymbolicName();
    setState(1493);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1492);
      match(CypherParser::SP);
    }
    setState(1495);
    match(CypherParser::T__8);
    setState(1497);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1496);
      match(CypherParser::SP);
    }
    setState(1499);
    oC_Expression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ParenthesizedExpressionContext ------------------------------------------------------------------

CypherParser::OC_ParenthesizedExpressionContext::OC_ParenthesizedExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_ExpressionContext* CypherParser::OC_ParenthesizedExpressionContext::oC_Expression() {
  return getRuleContext<CypherParser::OC_ExpressionContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_ParenthesizedExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_ParenthesizedExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_ParenthesizedExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_ParenthesizedExpression;
}


CypherParser::OC_ParenthesizedExpressionContext* CypherParser::oC_ParenthesizedExpression() {
  OC_ParenthesizedExpressionContext *_localctx = _tracker.createInstance<OC_ParenthesizedExpressionContext>(_ctx, getState());
  enterRule(_localctx, 194, CypherParser::RuleOC_ParenthesizedExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1501);
    match(CypherParser::T__1);
    setState(1503);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1502);
      match(CypherParser::SP);
    }
    setState(1505);
    oC_Expression();
    setState(1507);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1506);
      match(CypherParser::SP);
    }
    setState(1509);
    match(CypherParser::T__2);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_FunctionInvocationContext ------------------------------------------------------------------

CypherParser::OC_FunctionInvocationContext::OC_FunctionInvocationContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_FunctionNameContext* CypherParser::OC_FunctionInvocationContext::oC_FunctionName() {
  return getRuleContext<CypherParser::OC_FunctionNameContext>(0);
}

tree::TerminalNode* CypherParser::OC_FunctionInvocationContext::STAR() {
  return getToken(CypherParser::STAR, 0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_FunctionInvocationContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_FunctionInvocationContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::OC_FunctionInvocationContext::DISTINCT() {
  return getToken(CypherParser::DISTINCT, 0);
}

std::vector<CypherParser::OC_ExpressionContext *> CypherParser::OC_FunctionInvocationContext::oC_Expression() {
  return getRuleContexts<CypherParser::OC_ExpressionContext>();
}

CypherParser::OC_ExpressionContext* CypherParser::OC_FunctionInvocationContext::oC_Expression(size_t i) {
  return getRuleContext<CypherParser::OC_ExpressionContext>(i);
}


size_t CypherParser::OC_FunctionInvocationContext::getRuleIndex() const {
  return CypherParser::RuleOC_FunctionInvocation;
}


CypherParser::OC_FunctionInvocationContext* CypherParser::oC_FunctionInvocation() {
  OC_FunctionInvocationContext *_localctx = _tracker.createInstance<OC_FunctionInvocationContext>(_ctx, getState());
  enterRule(_localctx, 196, CypherParser::RuleOC_FunctionInvocation);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1560);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 267, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(1511);
      oC_FunctionName();
      setState(1513);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1512);
        match(CypherParser::SP);
      }
      setState(1515);
      match(CypherParser::T__1);
      setState(1517);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1516);
        match(CypherParser::SP);
      }
      setState(1519);
      match(CypherParser::STAR);
      setState(1521);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1520);
        match(CypherParser::SP);
      }
      setState(1523);
      match(CypherParser::T__2);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(1525);
      oC_FunctionName();
      setState(1527);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1526);
        match(CypherParser::SP);
      }
      setState(1529);
      match(CypherParser::T__1);
      setState(1531);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1530);
        match(CypherParser::SP);
      }
      setState(1537);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::DISTINCT) {
        setState(1533);
        match(CypherParser::DISTINCT);
        setState(1535);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1534);
          match(CypherParser::SP);
        }
      }
      setState(1556);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << CypherParser::T__1)
        | (1ULL << CypherParser::T__4)
        | (1ULL << CypherParser::T__7)
        | (1ULL << CypherParser::T__26))) != 0) || ((((_la - 91) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 91)) & ((1ULL << (CypherParser::NOT - 91))
        | (1ULL << (CypherParser::MINUS - 91))
        | (1ULL << (CypherParser::NULL_ - 91))
        | (1ULL << (CypherParser::TRUE - 91))
        | (1ULL << (CypherParser::FALSE - 91))
        | (1ULL << (CypherParser::EXISTS - 91))
        | (1ULL << (CypherParser::CASE - 91))
        | (1ULL << (CypherParser::StringLiteral - 91))
        | (1ULL << (CypherParser::DecimalInteger - 91))
        | (1ULL << (CypherParser::HexLetter - 91))
        | (1ULL << (CypherParser::RegularDecimalReal - 91))
        | (1ULL << (CypherParser::UnescapedSymbolicName - 91))
        | (1ULL << (CypherParser::EscapedSymbolicName - 91)))) != 0)) {
        setState(1539);
        oC_Expression();
        setState(1541);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1540);
          match(CypherParser::SP);
        }
        setState(1553);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == CypherParser::T__3) {
          setState(1543);
          match(CypherParser::T__3);
          setState(1545);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == CypherParser::SP) {
            setState(1544);
            match(CypherParser::SP);
          }
          setState(1547);
          oC_Expression();
          setState(1549);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == CypherParser::SP) {
            setState(1548);
            match(CypherParser::SP);
          }
          setState(1555);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
      }
      setState(1558);
      match(CypherParser::T__2);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_FunctionNameContext ------------------------------------------------------------------

CypherParser::OC_FunctionNameContext::OC_FunctionNameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SymbolicNameContext* CypherParser::OC_FunctionNameContext::oC_SymbolicName() {
  return getRuleContext<CypherParser::OC_SymbolicNameContext>(0);
}


size_t CypherParser::OC_FunctionNameContext::getRuleIndex() const {
  return CypherParser::RuleOC_FunctionName;
}


CypherParser::OC_FunctionNameContext* CypherParser::oC_FunctionName() {
  OC_FunctionNameContext *_localctx = _tracker.createInstance<OC_FunctionNameContext>(_ctx, getState());
  enterRule(_localctx, 198, CypherParser::RuleOC_FunctionName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1562);
    oC_SymbolicName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ExistentialSubqueryContext ------------------------------------------------------------------

CypherParser::OC_ExistentialSubqueryContext::OC_ExistentialSubqueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_ExistentialSubqueryContext::EXISTS() {
  return getToken(CypherParser::EXISTS, 0);
}

tree::TerminalNode* CypherParser::OC_ExistentialSubqueryContext::MATCH() {
  return getToken(CypherParser::MATCH, 0);
}

CypherParser::OC_PatternContext* CypherParser::OC_ExistentialSubqueryContext::oC_Pattern() {
  return getRuleContext<CypherParser::OC_PatternContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_ExistentialSubqueryContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_ExistentialSubqueryContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OC_WhereContext* CypherParser::OC_ExistentialSubqueryContext::oC_Where() {
  return getRuleContext<CypherParser::OC_WhereContext>(0);
}


size_t CypherParser::OC_ExistentialSubqueryContext::getRuleIndex() const {
  return CypherParser::RuleOC_ExistentialSubquery;
}


CypherParser::OC_ExistentialSubqueryContext* CypherParser::oC_ExistentialSubquery() {
  OC_ExistentialSubqueryContext *_localctx = _tracker.createInstance<OC_ExistentialSubqueryContext>(_ctx, getState());
  enterRule(_localctx, 200, CypherParser::RuleOC_ExistentialSubquery);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1564);
    match(CypherParser::EXISTS);
    setState(1566);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1565);
      match(CypherParser::SP);
    }
    setState(1568);
    match(CypherParser::T__7);
    setState(1570);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1569);
      match(CypherParser::SP);
    }
    setState(1572);
    match(CypherParser::MATCH);
    setState(1574);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 270, _ctx)) {
    case 1: {
      setState(1573);
      match(CypherParser::SP);
      break;
    }

    default:
      break;
    }
    setState(1576);
    oC_Pattern();
    setState(1581);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 272, _ctx)) {
    case 1: {
      setState(1578);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1577);
        match(CypherParser::SP);
      }
      setState(1580);
      oC_Where();
      break;
    }

    default:
      break;
    }
    setState(1584);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1583);
      match(CypherParser::SP);
    }
    setState(1586);
    match(CypherParser::T__9);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_PropertyLookupContext ------------------------------------------------------------------

CypherParser::OC_PropertyLookupContext::OC_PropertyLookupContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_PropertyKeyNameContext* CypherParser::OC_PropertyLookupContext::oC_PropertyKeyName() {
  return getRuleContext<CypherParser::OC_PropertyKeyNameContext>(0);
}

tree::TerminalNode* CypherParser::OC_PropertyLookupContext::SP() {
  return getToken(CypherParser::SP, 0);
}


size_t CypherParser::OC_PropertyLookupContext::getRuleIndex() const {
  return CypherParser::RuleOC_PropertyLookup;
}


CypherParser::OC_PropertyLookupContext* CypherParser::oC_PropertyLookup() {
  OC_PropertyLookupContext *_localctx = _tracker.createInstance<OC_PropertyLookupContext>(_ctx, getState());
  enterRule(_localctx, 202, CypherParser::RuleOC_PropertyLookup);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1588);
    match(CypherParser::T__25);
    setState(1590);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1589);
      match(CypherParser::SP);
    }

    setState(1592);
    oC_PropertyKeyName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_CaseExpressionContext ------------------------------------------------------------------

CypherParser::OC_CaseExpressionContext::OC_CaseExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_CaseExpressionContext::END() {
  return getToken(CypherParser::END, 0);
}

tree::TerminalNode* CypherParser::OC_CaseExpressionContext::ELSE() {
  return getToken(CypherParser::ELSE, 0);
}

std::vector<CypherParser::OC_ExpressionContext *> CypherParser::OC_CaseExpressionContext::oC_Expression() {
  return getRuleContexts<CypherParser::OC_ExpressionContext>();
}

CypherParser::OC_ExpressionContext* CypherParser::OC_CaseExpressionContext::oC_Expression(size_t i) {
  return getRuleContext<CypherParser::OC_ExpressionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_CaseExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_CaseExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::OC_CaseExpressionContext::CASE() {
  return getToken(CypherParser::CASE, 0);
}

std::vector<CypherParser::OC_CaseAlternativeContext *> CypherParser::OC_CaseExpressionContext::oC_CaseAlternative() {
  return getRuleContexts<CypherParser::OC_CaseAlternativeContext>();
}

CypherParser::OC_CaseAlternativeContext* CypherParser::OC_CaseExpressionContext::oC_CaseAlternative(size_t i) {
  return getRuleContext<CypherParser::OC_CaseAlternativeContext>(i);
}


size_t CypherParser::OC_CaseExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_CaseExpression;
}


CypherParser::OC_CaseExpressionContext* CypherParser::oC_CaseExpression() {
  OC_CaseExpressionContext *_localctx = _tracker.createInstance<OC_CaseExpressionContext>(_ctx, getState());
  enterRule(_localctx, 204, CypherParser::RuleOC_CaseExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1616);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 280, _ctx)) {
    case 1: {
      setState(1594);
      match(CypherParser::CASE);
      setState(1599); 
      _errHandler->sync(this);
      alt = 1;
      do {
        switch (alt) {
          case 1: {
                setState(1596);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == CypherParser::SP) {
                  setState(1595);
                  match(CypherParser::SP);
                }
                setState(1598);
                oC_CaseAlternative();
                break;
              }

        default:
          throw NoViableAltException(this);
        }
        setState(1601); 
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 276, _ctx);
      } while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER);
      break;
    }

    case 2: {
      setState(1603);
      match(CypherParser::CASE);
      setState(1605);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1604);
        match(CypherParser::SP);
      }
      setState(1607);
      oC_Expression();
      setState(1612); 
      _errHandler->sync(this);
      alt = 1;
      do {
        switch (alt) {
          case 1: {
                setState(1609);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == CypherParser::SP) {
                  setState(1608);
                  match(CypherParser::SP);
                }
                setState(1611);
                oC_CaseAlternative();
                break;
              }

        default:
          throw NoViableAltException(this);
        }
        setState(1614); 
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 279, _ctx);
      } while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER);
      break;
    }

    default:
      break;
    }
    setState(1626);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 283, _ctx)) {
    case 1: {
      setState(1619);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1618);
        match(CypherParser::SP);
      }
      setState(1621);
      match(CypherParser::ELSE);
      setState(1623);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1622);
        match(CypherParser::SP);
      }
      setState(1625);
      oC_Expression();
      break;
    }

    default:
      break;
    }
    setState(1629);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1628);
      match(CypherParser::SP);
    }
    setState(1631);
    match(CypherParser::END);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_CaseAlternativeContext ------------------------------------------------------------------

CypherParser::OC_CaseAlternativeContext::OC_CaseAlternativeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_CaseAlternativeContext::WHEN() {
  return getToken(CypherParser::WHEN, 0);
}

std::vector<CypherParser::OC_ExpressionContext *> CypherParser::OC_CaseAlternativeContext::oC_Expression() {
  return getRuleContexts<CypherParser::OC_ExpressionContext>();
}

CypherParser::OC_ExpressionContext* CypherParser::OC_CaseAlternativeContext::oC_Expression(size_t i) {
  return getRuleContext<CypherParser::OC_ExpressionContext>(i);
}

tree::TerminalNode* CypherParser::OC_CaseAlternativeContext::THEN() {
  return getToken(CypherParser::THEN, 0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_CaseAlternativeContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_CaseAlternativeContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_CaseAlternativeContext::getRuleIndex() const {
  return CypherParser::RuleOC_CaseAlternative;
}


CypherParser::OC_CaseAlternativeContext* CypherParser::oC_CaseAlternative() {
  OC_CaseAlternativeContext *_localctx = _tracker.createInstance<OC_CaseAlternativeContext>(_ctx, getState());
  enterRule(_localctx, 206, CypherParser::RuleOC_CaseAlternative);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1633);
    match(CypherParser::WHEN);
    setState(1635);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1634);
      match(CypherParser::SP);
    }
    setState(1637);
    oC_Expression();
    setState(1639);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1638);
      match(CypherParser::SP);
    }
    setState(1641);
    match(CypherParser::THEN);
    setState(1643);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1642);
      match(CypherParser::SP);
    }
    setState(1645);
    oC_Expression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_VariableContext ------------------------------------------------------------------

CypherParser::OC_VariableContext::OC_VariableContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SymbolicNameContext* CypherParser::OC_VariableContext::oC_SymbolicName() {
  return getRuleContext<CypherParser::OC_SymbolicNameContext>(0);
}


size_t CypherParser::OC_VariableContext::getRuleIndex() const {
  return CypherParser::RuleOC_Variable;
}


CypherParser::OC_VariableContext* CypherParser::oC_Variable() {
  OC_VariableContext *_localctx = _tracker.createInstance<OC_VariableContext>(_ctx, getState());
  enterRule(_localctx, 208, CypherParser::RuleOC_Variable);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1647);
    oC_SymbolicName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_NumberLiteralContext ------------------------------------------------------------------

CypherParser::OC_NumberLiteralContext::OC_NumberLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_DoubleLiteralContext* CypherParser::OC_NumberLiteralContext::oC_DoubleLiteral() {
  return getRuleContext<CypherParser::OC_DoubleLiteralContext>(0);
}

CypherParser::OC_IntegerLiteralContext* CypherParser::OC_NumberLiteralContext::oC_IntegerLiteral() {
  return getRuleContext<CypherParser::OC_IntegerLiteralContext>(0);
}


size_t CypherParser::OC_NumberLiteralContext::getRuleIndex() const {
  return CypherParser::RuleOC_NumberLiteral;
}


CypherParser::OC_NumberLiteralContext* CypherParser::oC_NumberLiteral() {
  OC_NumberLiteralContext *_localctx = _tracker.createInstance<OC_NumberLiteralContext>(_ctx, getState());
  enterRule(_localctx, 210, CypherParser::RuleOC_NumberLiteral);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1651);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::RegularDecimalReal: {
        enterOuterAlt(_localctx, 1);
        setState(1649);
        oC_DoubleLiteral();
        break;
      }

      case CypherParser::DecimalInteger: {
        enterOuterAlt(_localctx, 2);
        setState(1650);
        oC_IntegerLiteral();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ParameterContext ------------------------------------------------------------------

CypherParser::OC_ParameterContext::OC_ParameterContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SymbolicNameContext* CypherParser::OC_ParameterContext::oC_SymbolicName() {
  return getRuleContext<CypherParser::OC_SymbolicNameContext>(0);
}

tree::TerminalNode* CypherParser::OC_ParameterContext::DecimalInteger() {
  return getToken(CypherParser::DecimalInteger, 0);
}


size_t CypherParser::OC_ParameterContext::getRuleIndex() const {
  return CypherParser::RuleOC_Parameter;
}


CypherParser::OC_ParameterContext* CypherParser::oC_Parameter() {
  OC_ParameterContext *_localctx = _tracker.createInstance<OC_ParameterContext>(_ctx, getState());
  enterRule(_localctx, 212, CypherParser::RuleOC_Parameter);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1653);
    match(CypherParser::T__26);
    setState(1656);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::HexLetter:
      case CypherParser::UnescapedSymbolicName:
      case CypherParser::EscapedSymbolicName: {
        setState(1654);
        oC_SymbolicName();
        break;
      }

      case CypherParser::DecimalInteger: {
        setState(1655);
        match(CypherParser::DecimalInteger);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_PropertyExpressionContext ------------------------------------------------------------------

CypherParser::OC_PropertyExpressionContext::OC_PropertyExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_AtomContext* CypherParser::OC_PropertyExpressionContext::oC_Atom() {
  return getRuleContext<CypherParser::OC_AtomContext>(0);
}

CypherParser::OC_PropertyLookupContext* CypherParser::OC_PropertyExpressionContext::oC_PropertyLookup() {
  return getRuleContext<CypherParser::OC_PropertyLookupContext>(0);
}

tree::TerminalNode* CypherParser::OC_PropertyExpressionContext::SP() {
  return getToken(CypherParser::SP, 0);
}


size_t CypherParser::OC_PropertyExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_PropertyExpression;
}


CypherParser::OC_PropertyExpressionContext* CypherParser::oC_PropertyExpression() {
  OC_PropertyExpressionContext *_localctx = _tracker.createInstance<OC_PropertyExpressionContext>(_ctx, getState());
  enterRule(_localctx, 214, CypherParser::RuleOC_PropertyExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1658);
    oC_Atom();
    setState(1660);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1659);
      match(CypherParser::SP);
    }
    setState(1662);
    oC_PropertyLookup();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_PropertyKeyNameContext ------------------------------------------------------------------

CypherParser::OC_PropertyKeyNameContext::OC_PropertyKeyNameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SchemaNameContext* CypherParser::OC_PropertyKeyNameContext::oC_SchemaName() {
  return getRuleContext<CypherParser::OC_SchemaNameContext>(0);
}


size_t CypherParser::OC_PropertyKeyNameContext::getRuleIndex() const {
  return CypherParser::RuleOC_PropertyKeyName;
}


CypherParser::OC_PropertyKeyNameContext* CypherParser::oC_PropertyKeyName() {
  OC_PropertyKeyNameContext *_localctx = _tracker.createInstance<OC_PropertyKeyNameContext>(_ctx, getState());
  enterRule(_localctx, 216, CypherParser::RuleOC_PropertyKeyName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1664);
    oC_SchemaName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_IntegerLiteralContext ------------------------------------------------------------------

CypherParser::OC_IntegerLiteralContext::OC_IntegerLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_IntegerLiteralContext::DecimalInteger() {
  return getToken(CypherParser::DecimalInteger, 0);
}


size_t CypherParser::OC_IntegerLiteralContext::getRuleIndex() const {
  return CypherParser::RuleOC_IntegerLiteral;
}


CypherParser::OC_IntegerLiteralContext* CypherParser::oC_IntegerLiteral() {
  OC_IntegerLiteralContext *_localctx = _tracker.createInstance<OC_IntegerLiteralContext>(_ctx, getState());
  enterRule(_localctx, 218, CypherParser::RuleOC_IntegerLiteral);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1666);
    match(CypherParser::DecimalInteger);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_DoubleLiteralContext ------------------------------------------------------------------

CypherParser::OC_DoubleLiteralContext::OC_DoubleLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_DoubleLiteralContext::RegularDecimalReal() {
  return getToken(CypherParser::RegularDecimalReal, 0);
}


size_t CypherParser::OC_DoubleLiteralContext::getRuleIndex() const {
  return CypherParser::RuleOC_DoubleLiteral;
}


CypherParser::OC_DoubleLiteralContext* CypherParser::oC_DoubleLiteral() {
  OC_DoubleLiteralContext *_localctx = _tracker.createInstance<OC_DoubleLiteralContext>(_ctx, getState());
  enterRule(_localctx, 220, CypherParser::RuleOC_DoubleLiteral);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1668);
    match(CypherParser::RegularDecimalReal);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_SchemaNameContext ------------------------------------------------------------------

CypherParser::OC_SchemaNameContext::OC_SchemaNameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SymbolicNameContext* CypherParser::OC_SchemaNameContext::oC_SymbolicName() {
  return getRuleContext<CypherParser::OC_SymbolicNameContext>(0);
}


size_t CypherParser::OC_SchemaNameContext::getRuleIndex() const {
  return CypherParser::RuleOC_SchemaName;
}


CypherParser::OC_SchemaNameContext* CypherParser::oC_SchemaName() {
  OC_SchemaNameContext *_localctx = _tracker.createInstance<OC_SchemaNameContext>(_ctx, getState());
  enterRule(_localctx, 222, CypherParser::RuleOC_SchemaName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1670);
    oC_SymbolicName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_SymbolicNameContext ------------------------------------------------------------------

CypherParser::OC_SymbolicNameContext::OC_SymbolicNameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_SymbolicNameContext::UnescapedSymbolicName() {
  return getToken(CypherParser::UnescapedSymbolicName, 0);
}

tree::TerminalNode* CypherParser::OC_SymbolicNameContext::EscapedSymbolicName() {
  return getToken(CypherParser::EscapedSymbolicName, 0);
}

tree::TerminalNode* CypherParser::OC_SymbolicNameContext::HexLetter() {
  return getToken(CypherParser::HexLetter, 0);
}


size_t CypherParser::OC_SymbolicNameContext::getRuleIndex() const {
  return CypherParser::RuleOC_SymbolicName;
}


CypherParser::OC_SymbolicNameContext* CypherParser::oC_SymbolicName() {
  OC_SymbolicNameContext *_localctx = _tracker.createInstance<OC_SymbolicNameContext>(_ctx, getState());
  enterRule(_localctx, 224, CypherParser::RuleOC_SymbolicName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1676);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::UnescapedSymbolicName: {
        enterOuterAlt(_localctx, 1);
        setState(1672);
        match(CypherParser::UnescapedSymbolicName);
        break;
      }

      case CypherParser::EscapedSymbolicName: {
        enterOuterAlt(_localctx, 2);
        setState(1673);
        dynamic_cast<OC_SymbolicNameContext *>(_localctx)->escapedsymbolicnameToken = match(CypherParser::EscapedSymbolicName);
        if ((dynamic_cast<OC_SymbolicNameContext *>(_localctx)->escapedsymbolicnameToken != nullptr ? dynamic_cast<OC_SymbolicNameContext *>(_localctx)->escapedsymbolicnameToken->getText() : "") == "``") { notifyEmptyToken(dynamic_cast<OC_SymbolicNameContext *>(_localctx)->escapedsymbolicnameToken); }
        break;
      }

      case CypherParser::HexLetter: {
        enterOuterAlt(_localctx, 3);
        setState(1675);
        match(CypherParser::HexLetter);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_LeftArrowHeadContext ------------------------------------------------------------------

CypherParser::OC_LeftArrowHeadContext::OC_LeftArrowHeadContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t CypherParser::OC_LeftArrowHeadContext::getRuleIndex() const {
  return CypherParser::RuleOC_LeftArrowHead;
}


CypherParser::OC_LeftArrowHeadContext* CypherParser::oC_LeftArrowHead() {
  OC_LeftArrowHeadContext *_localctx = _tracker.createInstance<OC_LeftArrowHeadContext>(_ctx, getState());
  enterRule(_localctx, 226, CypherParser::RuleOC_LeftArrowHead);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1678);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << CypherParser::T__13)
      | (1ULL << CypherParser::T__27)
      | (1ULL << CypherParser::T__28)
      | (1ULL << CypherParser::T__29)
      | (1ULL << CypherParser::T__30))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_RightArrowHeadContext ------------------------------------------------------------------

CypherParser::OC_RightArrowHeadContext::OC_RightArrowHeadContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t CypherParser::OC_RightArrowHeadContext::getRuleIndex() const {
  return CypherParser::RuleOC_RightArrowHead;
}


CypherParser::OC_RightArrowHeadContext* CypherParser::oC_RightArrowHead() {
  OC_RightArrowHeadContext *_localctx = _tracker.createInstance<OC_RightArrowHeadContext>(_ctx, getState());
  enterRule(_localctx, 228, CypherParser::RuleOC_RightArrowHead);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1680);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << CypherParser::T__15)
      | (1ULL << CypherParser::T__31)
      | (1ULL << CypherParser::T__32)
      | (1ULL << CypherParser::T__33)
      | (1ULL << CypherParser::T__34))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_DashContext ------------------------------------------------------------------

CypherParser::OC_DashContext::OC_DashContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_DashContext::MINUS() {
  return getToken(CypherParser::MINUS, 0);
}


size_t CypherParser::OC_DashContext::getRuleIndex() const {
  return CypherParser::RuleOC_Dash;
}


CypherParser::OC_DashContext* CypherParser::oC_Dash() {
  OC_DashContext *_localctx = _tracker.createInstance<OC_DashContext>(_ctx, getState());
  enterRule(_localctx, 230, CypherParser::RuleOC_Dash);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1682);
    _la = _input->LA(1);
    if (!(((((_la - 36) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 36)) & ((1ULL << (CypherParser::T__35 - 36))
      | (1ULL << (CypherParser::T__36 - 36))
      | (1ULL << (CypherParser::T__37 - 36))
      | (1ULL << (CypherParser::T__38 - 36))
      | (1ULL << (CypherParser::T__39 - 36))
      | (1ULL << (CypherParser::T__40 - 36))
      | (1ULL << (CypherParser::T__41 - 36))
      | (1ULL << (CypherParser::T__42 - 36))
      | (1ULL << (CypherParser::T__43 - 36))
      | (1ULL << (CypherParser::T__44 - 36))
      | (1ULL << (CypherParser::T__45 - 36))
      | (1ULL << (CypherParser::MINUS - 36)))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

// Static vars and initialization.
std::vector<dfa::DFA> CypherParser::_decisionToDFA;
atn::PredictionContextCache CypherParser::_sharedContextCache;

// We own the ATN which in turn owns the ATN states.
atn::ATN CypherParser::_atn;
std::vector<uint16_t> CypherParser::_serializedATN;

std::vector<std::string> CypherParser::_ruleNames = {
  "oC_Cypher", "kU_CopyCSV", "kU_CopyNPY", "kU_FilePaths", "kU_ParsingOptions", 
  "kU_ParsingOption", "kU_DDL", "kU_CreateNode", "kU_CreateRel", "kU_DropTable", 
  "kU_AlterTable", "kU_AlterOptions", "kU_AddProperty", "kU_DropProperty", 
  "kU_RenameTable", "kU_RenameProperty", "kU_PropertyDefinitions", "kU_PropertyDefinition", 
  "kU_CreateNodeConstraint", "kU_DataType", "kU_ListIdentifiers", "kU_ListIdentifier", 
  "oC_AnyCypherOption", "oC_Explain", "oC_Profile", "oC_Statement", "oC_Query", 
  "oC_RegularQuery", "oC_Union", "oC_SingleQuery", "oC_SinglePartQuery", 
  "oC_MultiPartQuery", "kU_QueryPart", "oC_UpdatingClause", "oC_ReadingClause", 
  "oC_Match", "oC_Unwind", "oC_Create", "oC_Set", "oC_SetItem", "oC_Delete", 
  "oC_With", "oC_Return", "oC_ProjectionBody", "oC_ProjectionItems", "oC_ProjectionItem", 
  "oC_Order", "oC_Skip", "oC_Limit", "oC_SortItem", "oC_Where", "oC_Pattern", 
  "oC_PatternPart", "oC_AnonymousPatternPart", "oC_PatternElement", "oC_NodePattern", 
  "oC_PatternElementChain", "oC_RelationshipPattern", "oC_RelationshipDetail", 
  "kU_Properties", "oC_RelationshipTypes", "oC_NodeLabels", "oC_NodeLabel", 
  "oC_RangeLiteral", "oC_LabelName", "oC_RelTypeName", "oC_Expression", 
  "oC_OrExpression", "oC_XorExpression", "oC_AndExpression", "oC_NotExpression", 
  "oC_ComparisonExpression", "kU_ComparisonOperator", "kU_BitwiseOrOperatorExpression", 
  "kU_BitwiseAndOperatorExpression", "kU_BitShiftOperatorExpression", "kU_BitShiftOperator", 
  "oC_AddOrSubtractExpression", "kU_AddOrSubtractOperator", "oC_MultiplyDivideModuloExpression", 
  "kU_MultiplyDivideModuloOperator", "oC_PowerOfExpression", "oC_UnaryAddSubtractOrFactorialExpression", 
  "oC_StringListNullOperatorExpression", "oC_ListOperatorExpression", "kU_ListExtractOperatorExpression", 
  "kU_ListSliceOperatorExpression", "oC_StringOperatorExpression", "oC_RegularExpression", 
  "oC_NullOperatorExpression", "oC_PropertyOrLabelsExpression", "oC_Atom", 
  "oC_Literal", "oC_BooleanLiteral", "oC_ListLiteral", "kU_StructLiteral", 
  "kU_StructField", "oC_ParenthesizedExpression", "oC_FunctionInvocation", 
  "oC_FunctionName", "oC_ExistentialSubquery", "oC_PropertyLookup", "oC_CaseExpression", 
  "oC_CaseAlternative", "oC_Variable", "oC_NumberLiteral", "oC_Parameter", 
  "oC_PropertyExpression", "oC_PropertyKeyName", "oC_IntegerLiteral", "oC_DoubleLiteral", 
  "oC_SchemaName", "oC_SymbolicName", "oC_LeftArrowHead", "oC_RightArrowHead", 
  "oC_Dash"
};

std::vector<std::string> CypherParser::_literalNames = {
  "", "';'", "'('", "')'", "','", "'['", "']'", "'='", "'{'", "':'", "'}'", 
  "'|'", "'..'", "'<>'", "'<'", "'<='", "'>'", "'>='", "'&'", "'>>'", "'<<'", 
  "'+'", "'/'", "'%'", "'^'", "'=~'", "'.'", "'$'", "'\u27E8'", "'\u3008'", 
  "'\uFE64'", "'\uFF1C'", "'\u27E9'", "'\u3009'", "'\uFE65'", "'\uFF1E'", 
  "'\u00AD'", "'\u2010'", "'\u2011'", "'\u2012'", "'\u2013'", "'\u2014'", 
  "'\u2015'", "'\u2212'", "'\uFE58'", "'\uFE63'", "'\uFF0D'", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "'*'", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "'!='", "'-'", "'!'", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "'0'"
};

std::vector<std::string> CypherParser::_symbolicNames = {
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "GLOB", "COPY", "FROM", "NPY", 
  "COLUMN", "NODE", "TABLE", "DROP", "ALTER", "DEFAULT", "RENAME", "ADD", 
  "PRIMARY", "KEY", "REL", "TO", "EXPLAIN", "PROFILE", "UNION", "ALL", "OPTIONAL", 
  "MATCH", "UNWIND", "CREATE", "SET", "DELETE", "WITH", "RETURN", "DISTINCT", 
  "STAR", "AS", "ORDER", "BY", "L_SKIP", "LIMIT", "ASCENDING", "ASC", "DESCENDING", 
  "DESC", "WHERE", "SHORTEST", "OR", "XOR", "AND", "NOT", "INVALID_NOT_EQUAL", 
  "MINUS", "FACTORIAL", "STARTS", "ENDS", "CONTAINS", "IS", "NULL_", "TRUE", 
  "FALSE", "EXISTS", "CASE", "ELSE", "END", "WHEN", "THEN", "StringLiteral", 
  "EscapedChar", "DecimalInteger", "HexLetter", "HexDigit", "Digit", "NonZeroDigit", 
  "NonZeroOctDigit", "ZeroDigit", "RegularDecimalReal", "UnescapedSymbolicName", 
  "IdentifierStart", "IdentifierPart", "EscapedSymbolicName", "SP", "WHITESPACE", 
  "Comment", "Unknown"
};

dfa::Vocabulary CypherParser::_vocabulary(_literalNames, _symbolicNames);

std::vector<std::string> CypherParser::_tokenNames;

CypherParser::Initializer::Initializer() {
	for (size_t i = 0; i < _symbolicNames.size(); ++i) {
		std::string name = _vocabulary.getLiteralName(i);
		if (name.empty()) {
			name = _vocabulary.getSymbolicName(i);
		}

		if (name.empty()) {
			_tokenNames.push_back("<INVALID>");
		} else {
      _tokenNames.push_back(name);
    }
	}

  _serializedATN = {
    0x3, 0x608b, 0xa72a, 0x8133, 0xb9ed, 0x417c, 0x3be7, 0x7786, 0x5964, 
    0x3, 0x7f, 0x697, 0x4, 0x2, 0x9, 0x2, 0x4, 0x3, 0x9, 0x3, 0x4, 0x4, 
    0x9, 0x4, 0x4, 0x5, 0x9, 0x5, 0x4, 0x6, 0x9, 0x6, 0x4, 0x7, 0x9, 0x7, 
    0x4, 0x8, 0x9, 0x8, 0x4, 0x9, 0x9, 0x9, 0x4, 0xa, 0x9, 0xa, 0x4, 0xb, 
    0x9, 0xb, 0x4, 0xc, 0x9, 0xc, 0x4, 0xd, 0x9, 0xd, 0x4, 0xe, 0x9, 0xe, 
    0x4, 0xf, 0x9, 0xf, 0x4, 0x10, 0x9, 0x10, 0x4, 0x11, 0x9, 0x11, 0x4, 
    0x12, 0x9, 0x12, 0x4, 0x13, 0x9, 0x13, 0x4, 0x14, 0x9, 0x14, 0x4, 0x15, 
    0x9, 0x15, 0x4, 0x16, 0x9, 0x16, 0x4, 0x17, 0x9, 0x17, 0x4, 0x18, 0x9, 
    0x18, 0x4, 0x19, 0x9, 0x19, 0x4, 0x1a, 0x9, 0x1a, 0x4, 0x1b, 0x9, 0x1b, 
    0x4, 0x1c, 0x9, 0x1c, 0x4, 0x1d, 0x9, 0x1d, 0x4, 0x1e, 0x9, 0x1e, 0x4, 
    0x1f, 0x9, 0x1f, 0x4, 0x20, 0x9, 0x20, 0x4, 0x21, 0x9, 0x21, 0x4, 0x22, 
    0x9, 0x22, 0x4, 0x23, 0x9, 0x23, 0x4, 0x24, 0x9, 0x24, 0x4, 0x25, 0x9, 
    0x25, 0x4, 0x26, 0x9, 0x26, 0x4, 0x27, 0x9, 0x27, 0x4, 0x28, 0x9, 0x28, 
    0x4, 0x29, 0x9, 0x29, 0x4, 0x2a, 0x9, 0x2a, 0x4, 0x2b, 0x9, 0x2b, 0x4, 
    0x2c, 0x9, 0x2c, 0x4, 0x2d, 0x9, 0x2d, 0x4, 0x2e, 0x9, 0x2e, 0x4, 0x2f, 
    0x9, 0x2f, 0x4, 0x30, 0x9, 0x30, 0x4, 0x31, 0x9, 0x31, 0x4, 0x32, 0x9, 
    0x32, 0x4, 0x33, 0x9, 0x33, 0x4, 0x34, 0x9, 0x34, 0x4, 0x35, 0x9, 0x35, 
    0x4, 0x36, 0x9, 0x36, 0x4, 0x37, 0x9, 0x37, 0x4, 0x38, 0x9, 0x38, 0x4, 
    0x39, 0x9, 0x39, 0x4, 0x3a, 0x9, 0x3a, 0x4, 0x3b, 0x9, 0x3b, 0x4, 0x3c, 
    0x9, 0x3c, 0x4, 0x3d, 0x9, 0x3d, 0x4, 0x3e, 0x9, 0x3e, 0x4, 0x3f, 0x9, 
    0x3f, 0x4, 0x40, 0x9, 0x40, 0x4, 0x41, 0x9, 0x41, 0x4, 0x42, 0x9, 0x42, 
    0x4, 0x43, 0x9, 0x43, 0x4, 0x44, 0x9, 0x44, 0x4, 0x45, 0x9, 0x45, 0x4, 
    0x46, 0x9, 0x46, 0x4, 0x47, 0x9, 0x47, 0x4, 0x48, 0x9, 0x48, 0x4, 0x49, 
    0x9, 0x49, 0x4, 0x4a, 0x9, 0x4a, 0x4, 0x4b, 0x9, 0x4b, 0x4, 0x4c, 0x9, 
    0x4c, 0x4, 0x4d, 0x9, 0x4d, 0x4, 0x4e, 0x9, 0x4e, 0x4, 0x4f, 0x9, 0x4f, 
    0x4, 0x50, 0x9, 0x50, 0x4, 0x51, 0x9, 0x51, 0x4, 0x52, 0x9, 0x52, 0x4, 
    0x53, 0x9, 0x53, 0x4, 0x54, 0x9, 0x54, 0x4, 0x55, 0x9, 0x55, 0x4, 0x56, 
    0x9, 0x56, 0x4, 0x57, 0x9, 0x57, 0x4, 0x58, 0x9, 0x58, 0x4, 0x59, 0x9, 
    0x59, 0x4, 0x5a, 0x9, 0x5a, 0x4, 0x5b, 0x9, 0x5b, 0x4, 0x5c, 0x9, 0x5c, 
    0x4, 0x5d, 0x9, 0x5d, 0x4, 0x5e, 0x9, 0x5e, 0x4, 0x5f, 0x9, 0x5f, 0x4, 
    0x60, 0x9, 0x60, 0x4, 0x61, 0x9, 0x61, 0x4, 0x62, 0x9, 0x62, 0x4, 0x63, 
    0x9, 0x63, 0x4, 0x64, 0x9, 0x64, 0x4, 0x65, 0x9, 0x65, 0x4, 0x66, 0x9, 
    0x66, 0x4, 0x67, 0x9, 0x67, 0x4, 0x68, 0x9, 0x68, 0x4, 0x69, 0x9, 0x69, 
    0x4, 0x6a, 0x9, 0x6a, 0x4, 0x6b, 0x9, 0x6b, 0x4, 0x6c, 0x9, 0x6c, 0x4, 
    0x6d, 0x9, 0x6d, 0x4, 0x6e, 0x9, 0x6e, 0x4, 0x6f, 0x9, 0x6f, 0x4, 0x70, 
    0x9, 0x70, 0x4, 0x71, 0x9, 0x71, 0x4, 0x72, 0x9, 0x72, 0x4, 0x73, 0x9, 
    0x73, 0x4, 0x74, 0x9, 0x74, 0x4, 0x75, 0x9, 0x75, 0x3, 0x2, 0x5, 0x2, 
    0xec, 0xa, 0x2, 0x3, 0x2, 0x5, 0x2, 0xef, 0xa, 0x2, 0x3, 0x2, 0x5, 0x2, 
    0xf2, 0xa, 0x2, 0x3, 0x2, 0x3, 0x2, 0x3, 0x2, 0x3, 0x2, 0x5, 0x2, 0xf8, 
    0xa, 0x2, 0x3, 0x2, 0x5, 0x2, 0xfb, 0xa, 0x2, 0x3, 0x2, 0x5, 0x2, 0xfe, 
    0xa, 0x2, 0x3, 0x2, 0x5, 0x2, 0x101, 0xa, 0x2, 0x3, 0x2, 0x3, 0x2, 0x3, 
    0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 
    0x3, 0x5, 0x3, 0x10d, 0xa, 0x3, 0x3, 0x3, 0x3, 0x3, 0x5, 0x3, 0x111, 
    0xa, 0x3, 0x3, 0x3, 0x3, 0x3, 0x5, 0x3, 0x115, 0xa, 0x3, 0x3, 0x3, 0x3, 
    0x3, 0x5, 0x3, 0x119, 0xa, 0x3, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 
    0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x5, 0x4, 0x123, 0xa, 0x4, 0x3, 
    0x4, 0x3, 0x4, 0x5, 0x4, 0x127, 0xa, 0x4, 0x3, 0x4, 0x3, 0x4, 0x5, 0x4, 
    0x12b, 0xa, 0x4, 0x3, 0x4, 0x7, 0x4, 0x12e, 0xa, 0x4, 0xc, 0x4, 0xe, 
    0x4, 0x131, 0xb, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 
    0x3, 0x4, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x13b, 0xa, 0x5, 0x3, 0x5, 0x3, 
    0x5, 0x5, 0x5, 0x13f, 0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x143, 
    0xa, 0x5, 0x3, 0x5, 0x7, 0x5, 0x146, 0xa, 0x5, 0xc, 0x5, 0xe, 0x5, 0x149, 
    0xb, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x14f, 0xa, 
    0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x153, 0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 
    0x5, 0x5, 0x157, 0xa, 0x5, 0x3, 0x5, 0x5, 0x5, 0x15a, 0xa, 0x5, 0x3, 
    0x6, 0x3, 0x6, 0x5, 0x6, 0x15e, 0xa, 0x6, 0x3, 0x6, 0x3, 0x6, 0x5, 0x6, 
    0x162, 0xa, 0x6, 0x3, 0x6, 0x7, 0x6, 0x165, 0xa, 0x6, 0xc, 0x6, 0xe, 
    0x6, 0x168, 0xb, 0x6, 0x3, 0x7, 0x3, 0x7, 0x5, 0x7, 0x16c, 0xa, 0x7, 
    0x3, 0x7, 0x3, 0x7, 0x5, 0x7, 0x170, 0xa, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 
    0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x5, 0x8, 0x178, 0xa, 0x8, 0x3, 0x9, 
    0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 
    0x5, 0x9, 0x182, 0xa, 0x9, 0x3, 0x9, 0x3, 0x9, 0x5, 0x9, 0x186, 0xa, 
    0x9, 0x3, 0x9, 0x3, 0x9, 0x5, 0x9, 0x18a, 0xa, 0x9, 0x3, 0x9, 0x3, 0x9, 
    0x5, 0x9, 0x18e, 0xa, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x5, 0x9, 0x193, 
    0xa, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 
    0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x5, 0xa, 0x19f, 0xa, 0xa, 0x3, 
    0xa, 0x3, 0xa, 0x5, 0xa, 0x1a3, 0xa, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 
    0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x5, 0xa, 0x1ad, 0xa, 
    0xa, 0x3, 0xa, 0x3, 0xa, 0x5, 0xa, 0x1b1, 0xa, 0xa, 0x3, 0xa, 0x3, 0xa, 
    0x5, 0xa, 0x1b5, 0xa, 0xa, 0x5, 0xa, 0x1b7, 0xa, 0xa, 0x3, 0xa, 0x3, 
    0xa, 0x5, 0xa, 0x1bb, 0xa, 0xa, 0x3, 0xa, 0x3, 0xa, 0x5, 0xa, 0x1bf, 
    0xa, 0xa, 0x5, 0xa, 0x1c1, 0xa, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xb, 0x3, 
    0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xc, 0x3, 0xc, 0x3, 
    0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xd, 0x3, 
    0xd, 0x3, 0xd, 0x3, 0xd, 0x5, 0xd, 0x1d7, 0xa, 0xd, 0x3, 0xe, 0x3, 0xe, 
    0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 
    0x5, 0xe, 0x1e2, 0xa, 0xe, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 
    0x10, 0x3, 0x10, 0x3, 0x10, 0x3, 0x10, 0x3, 0x10, 0x3, 0x10, 0x3, 0x11, 
    0x3, 0x11, 0x3, 0x11, 0x3, 0x11, 0x3, 0x11, 0x3, 0x11, 0x3, 0x11, 0x3, 
    0x11, 0x3, 0x12, 0x3, 0x12, 0x5, 0x12, 0x1f8, 0xa, 0x12, 0x3, 0x12, 
    0x3, 0x12, 0x5, 0x12, 0x1fc, 0xa, 0x12, 0x3, 0x12, 0x7, 0x12, 0x1ff, 
    0xa, 0x12, 0xc, 0x12, 0xe, 0x12, 0x202, 0xb, 0x12, 0x3, 0x13, 0x3, 0x13, 
    0x3, 0x13, 0x3, 0x13, 0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 0x5, 
    0x14, 0x20c, 0xa, 0x14, 0x3, 0x14, 0x3, 0x14, 0x5, 0x14, 0x210, 0xa, 
    0x14, 0x3, 0x14, 0x3, 0x14, 0x5, 0x14, 0x214, 0xa, 0x14, 0x3, 0x14, 
    0x3, 0x14, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 
    0x15, 0x5, 0x15, 0x21e, 0xa, 0x15, 0x3, 0x15, 0x3, 0x15, 0x5, 0x15, 
    0x222, 0xa, 0x15, 0x3, 0x15, 0x3, 0x15, 0x5, 0x15, 0x226, 0xa, 0x15, 
    0x3, 0x15, 0x3, 0x15, 0x5, 0x15, 0x22a, 0xa, 0x15, 0x3, 0x16, 0x3, 0x16, 
    0x7, 0x16, 0x22e, 0xa, 0x16, 0xc, 0x16, 0xe, 0x16, 0x231, 0xb, 0x16, 
    0x3, 0x17, 0x3, 0x17, 0x5, 0x17, 0x235, 0xa, 0x17, 0x3, 0x17, 0x3, 0x17, 
    0x3, 0x18, 0x3, 0x18, 0x5, 0x18, 0x23b, 0xa, 0x18, 0x3, 0x19, 0x3, 0x19, 
    0x3, 0x1a, 0x3, 0x1a, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1c, 0x3, 0x1c, 0x3, 
    0x1d, 0x3, 0x1d, 0x5, 0x1d, 0x247, 0xa, 0x1d, 0x3, 0x1d, 0x7, 0x1d, 
    0x24a, 0xa, 0x1d, 0xc, 0x1d, 0xe, 0x1d, 0x24d, 0xb, 0x1d, 0x3, 0x1d, 
    0x3, 0x1d, 0x5, 0x1d, 0x251, 0xa, 0x1d, 0x6, 0x1d, 0x253, 0xa, 0x1d, 
    0xd, 0x1d, 0xe, 0x1d, 0x254, 0x3, 0x1d, 0x3, 0x1d, 0x3, 0x1d, 0x5, 0x1d, 
    0x25a, 0xa, 0x1d, 0x3, 0x1e, 0x3, 0x1e, 0x3, 0x1e, 0x3, 0x1e, 0x5, 0x1e, 
    0x260, 0xa, 0x1e, 0x3, 0x1e, 0x3, 0x1e, 0x3, 0x1e, 0x5, 0x1e, 0x265, 
    0xa, 0x1e, 0x3, 0x1e, 0x5, 0x1e, 0x268, 0xa, 0x1e, 0x3, 0x1f, 0x3, 0x1f, 
    0x5, 0x1f, 0x26c, 0xa, 0x1f, 0x3, 0x20, 0x3, 0x20, 0x5, 0x20, 0x270, 
    0xa, 0x20, 0x7, 0x20, 0x272, 0xa, 0x20, 0xc, 0x20, 0xe, 0x20, 0x275, 
    0xb, 0x20, 0x3, 0x20, 0x3, 0x20, 0x3, 0x20, 0x5, 0x20, 0x27a, 0xa, 0x20, 
    0x7, 0x20, 0x27c, 0xa, 0x20, 0xc, 0x20, 0xe, 0x20, 0x27f, 0xb, 0x20, 
    0x3, 0x20, 0x3, 0x20, 0x5, 0x20, 0x283, 0xa, 0x20, 0x3, 0x20, 0x7, 0x20, 
    0x286, 0xa, 0x20, 0xc, 0x20, 0xe, 0x20, 0x289, 0xb, 0x20, 0x3, 0x20, 
    0x5, 0x20, 0x28c, 0xa, 0x20, 0x3, 0x20, 0x5, 0x20, 0x28f, 0xa, 0x20, 
    0x3, 0x20, 0x3, 0x20, 0x5, 0x20, 0x293, 0xa, 0x20, 0x7, 0x20, 0x295, 
    0xa, 0x20, 0xc, 0x20, 0xe, 0x20, 0x298, 0xb, 0x20, 0x3, 0x20, 0x5, 0x20, 
    0x29b, 0xa, 0x20, 0x3, 0x21, 0x3, 0x21, 0x5, 0x21, 0x29f, 0xa, 0x21, 
    0x6, 0x21, 0x2a1, 0xa, 0x21, 0xd, 0x21, 0xe, 0x21, 0x2a2, 0x3, 0x21, 
    0x3, 0x21, 0x3, 0x22, 0x3, 0x22, 0x5, 0x22, 0x2a9, 0xa, 0x22, 0x7, 0x22, 
    0x2ab, 0xa, 0x22, 0xc, 0x22, 0xe, 0x22, 0x2ae, 0xb, 0x22, 0x3, 0x22, 
    0x3, 0x22, 0x5, 0x22, 0x2b2, 0xa, 0x22, 0x7, 0x22, 0x2b4, 0xa, 0x22, 
    0xc, 0x22, 0xe, 0x22, 0x2b7, 0xb, 0x22, 0x3, 0x22, 0x3, 0x22, 0x3, 0x23, 
    0x3, 0x23, 0x3, 0x23, 0x5, 0x23, 0x2be, 0xa, 0x23, 0x3, 0x24, 0x3, 0x24, 
    0x5, 0x24, 0x2c2, 0xa, 0x24, 0x3, 0x25, 0x3, 0x25, 0x5, 0x25, 0x2c6, 
    0xa, 0x25, 0x3, 0x25, 0x3, 0x25, 0x5, 0x25, 0x2ca, 0xa, 0x25, 0x3, 0x25, 
    0x3, 0x25, 0x5, 0x25, 0x2ce, 0xa, 0x25, 0x3, 0x25, 0x5, 0x25, 0x2d1, 
    0xa, 0x25, 0x3, 0x26, 0x3, 0x26, 0x5, 0x26, 0x2d5, 0xa, 0x26, 0x3, 0x26, 
    0x3, 0x26, 0x3, 0x26, 0x3, 0x26, 0x3, 0x26, 0x3, 0x26, 0x3, 0x27, 0x3, 
    0x27, 0x5, 0x27, 0x2df, 0xa, 0x27, 0x3, 0x27, 0x3, 0x27, 0x3, 0x28, 
    0x3, 0x28, 0x5, 0x28, 0x2e5, 0xa, 0x28, 0x3, 0x28, 0x3, 0x28, 0x5, 0x28, 
    0x2e9, 0xa, 0x28, 0x3, 0x28, 0x3, 0x28, 0x5, 0x28, 0x2ed, 0xa, 0x28, 
    0x3, 0x28, 0x7, 0x28, 0x2f0, 0xa, 0x28, 0xc, 0x28, 0xe, 0x28, 0x2f3, 
    0xb, 0x28, 0x3, 0x29, 0x3, 0x29, 0x5, 0x29, 0x2f7, 0xa, 0x29, 0x3, 0x29, 
    0x3, 0x29, 0x5, 0x29, 0x2fb, 0xa, 0x29, 0x3, 0x29, 0x3, 0x29, 0x3, 0x2a, 
    0x3, 0x2a, 0x5, 0x2a, 0x301, 0xa, 0x2a, 0x3, 0x2a, 0x3, 0x2a, 0x5, 0x2a, 
    0x305, 0xa, 0x2a, 0x3, 0x2a, 0x3, 0x2a, 0x5, 0x2a, 0x309, 0xa, 0x2a, 
    0x3, 0x2a, 0x7, 0x2a, 0x30c, 0xa, 0x2a, 0xc, 0x2a, 0xe, 0x2a, 0x30f, 
    0xb, 0x2a, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x5, 0x2b, 0x314, 0xa, 0x2b, 
    0x3, 0x2b, 0x5, 0x2b, 0x317, 0xa, 0x2b, 0x3, 0x2c, 0x3, 0x2c, 0x3, 0x2c, 
    0x3, 0x2d, 0x5, 0x2d, 0x31d, 0xa, 0x2d, 0x3, 0x2d, 0x5, 0x2d, 0x320, 
    0xa, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 0x5, 0x2d, 0x326, 
    0xa, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 0x5, 0x2d, 0x32a, 0xa, 0x2d, 0x3, 0x2d, 
    0x3, 0x2d, 0x5, 0x2d, 0x32e, 0xa, 0x2d, 0x3, 0x2e, 0x3, 0x2e, 0x5, 0x2e, 
    0x332, 0xa, 0x2e, 0x3, 0x2e, 0x3, 0x2e, 0x5, 0x2e, 0x336, 0xa, 0x2e, 
    0x3, 0x2e, 0x7, 0x2e, 0x339, 0xa, 0x2e, 0xc, 0x2e, 0xe, 0x2e, 0x33c, 
    0xb, 0x2e, 0x3, 0x2e, 0x3, 0x2e, 0x5, 0x2e, 0x340, 0xa, 0x2e, 0x3, 0x2e, 
    0x3, 0x2e, 0x5, 0x2e, 0x344, 0xa, 0x2e, 0x3, 0x2e, 0x7, 0x2e, 0x347, 
    0xa, 0x2e, 0xc, 0x2e, 0xe, 0x2e, 0x34a, 0xb, 0x2e, 0x5, 0x2e, 0x34c, 
    0xa, 0x2e, 0x3, 0x2f, 0x3, 0x2f, 0x3, 0x2f, 0x3, 0x2f, 0x3, 0x2f, 0x3, 
    0x2f, 0x3, 0x2f, 0x5, 0x2f, 0x355, 0xa, 0x2f, 0x3, 0x30, 0x3, 0x30, 
    0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 0x5, 0x30, 0x35e, 
    0xa, 0x30, 0x3, 0x30, 0x7, 0x30, 0x361, 0xa, 0x30, 0xc, 0x30, 0xe, 0x30, 
    0x364, 0xb, 0x30, 0x3, 0x31, 0x3, 0x31, 0x3, 0x31, 0x3, 0x31, 0x3, 0x32, 
    0x3, 0x32, 0x3, 0x32, 0x3, 0x32, 0x3, 0x33, 0x3, 0x33, 0x5, 0x33, 0x370, 
    0xa, 0x33, 0x3, 0x33, 0x5, 0x33, 0x373, 0xa, 0x33, 0x3, 0x34, 0x3, 0x34, 
    0x3, 0x34, 0x3, 0x34, 0x3, 0x35, 0x3, 0x35, 0x5, 0x35, 0x37b, 0xa, 0x35, 
    0x3, 0x35, 0x3, 0x35, 0x5, 0x35, 0x37f, 0xa, 0x35, 0x3, 0x35, 0x7, 0x35, 
    0x382, 0xa, 0x35, 0xc, 0x35, 0xe, 0x35, 0x385, 0xb, 0x35, 0x3, 0x36, 
    0x3, 0x36, 0x3, 0x37, 0x3, 0x37, 0x3, 0x38, 0x3, 0x38, 0x5, 0x38, 0x38d, 
    0xa, 0x38, 0x3, 0x38, 0x7, 0x38, 0x390, 0xa, 0x38, 0xc, 0x38, 0xe, 0x38, 
    0x393, 0xb, 0x38, 0x3, 0x38, 0x3, 0x38, 0x3, 0x38, 0x3, 0x38, 0x5, 0x38, 
    0x399, 0xa, 0x38, 0x3, 0x39, 0x3, 0x39, 0x5, 0x39, 0x39d, 0xa, 0x39, 
    0x3, 0x39, 0x3, 0x39, 0x5, 0x39, 0x3a1, 0xa, 0x39, 0x5, 0x39, 0x3a3, 
    0xa, 0x39, 0x3, 0x39, 0x3, 0x39, 0x5, 0x39, 0x3a7, 0xa, 0x39, 0x5, 0x39, 
    0x3a9, 0xa, 0x39, 0x3, 0x39, 0x3, 0x39, 0x5, 0x39, 0x3ad, 0xa, 0x39, 
    0x5, 0x39, 0x3af, 0xa, 0x39, 0x3, 0x39, 0x3, 0x39, 0x5, 0x39, 0x3b3, 
    0xa, 0x39, 0x3, 0x39, 0x3, 0x39, 0x5, 0x39, 0x3b7, 0xa, 0x39, 0x5, 0x39, 
    0x3b9, 0xa, 0x39, 0x3, 0x39, 0x3, 0x39, 0x5, 0x39, 0x3bd, 0xa, 0x39, 
    0x5, 0x39, 0x3bf, 0xa, 0x39, 0x3, 0x39, 0x3, 0x39, 0x5, 0x39, 0x3c3, 
    0xa, 0x39, 0x5, 0x39, 0x3c5, 0xa, 0x39, 0x3, 0x39, 0x5, 0x39, 0x3c8, 
    0xa, 0x39, 0x3, 0x3a, 0x3, 0x3a, 0x5, 0x3a, 0x3cc, 0xa, 0x3a, 0x3, 0x3a, 
    0x3, 0x3a, 0x3, 0x3b, 0x3, 0x3b, 0x5, 0x3b, 0x3d2, 0xa, 0x3b, 0x3, 0x3b, 
    0x3, 0x3b, 0x5, 0x3b, 0x3d6, 0xa, 0x3b, 0x3, 0x3b, 0x5, 0x3b, 0x3d9, 
    0xa, 0x3b, 0x3, 0x3b, 0x5, 0x3b, 0x3dc, 0xa, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 
    0x3, 0x3b, 0x3, 0x3b, 0x5, 0x3b, 0x3e2, 0xa, 0x3b, 0x3, 0x3b, 0x5, 0x3b, 
    0x3e5, 0xa, 0x3b, 0x3, 0x3b, 0x5, 0x3b, 0x3e8, 0xa, 0x3b, 0x3, 0x3b, 
    0x3, 0x3b, 0x5, 0x3b, 0x3ec, 0xa, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 
    0x3, 0x3b, 0x5, 0x3b, 0x3f2, 0xa, 0x3b, 0x3, 0x3b, 0x5, 0x3b, 0x3f5, 
    0xa, 0x3b, 0x3, 0x3b, 0x5, 0x3b, 0x3f8, 0xa, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 
    0x5, 0x3b, 0x3fc, 0xa, 0x3b, 0x3, 0x3c, 0x3, 0x3c, 0x5, 0x3c, 0x400, 
    0xa, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x5, 0x3c, 0x404, 0xa, 0x3c, 0x5, 0x3c, 
    0x406, 0xa, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x5, 0x3c, 0x40a, 0xa, 0x3c, 
    0x5, 0x3c, 0x40c, 0xa, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x5, 0x3c, 0x410, 
    0xa, 0x3c, 0x5, 0x3c, 0x412, 0xa, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x5, 0x3c, 
    0x416, 0xa, 0x3c, 0x5, 0x3c, 0x418, 0xa, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 
    0x3, 0x3d, 0x3, 0x3d, 0x5, 0x3d, 0x41e, 0xa, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 
    0x5, 0x3d, 0x422, 0xa, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x5, 0x3d, 0x426, 
    0xa, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x5, 0x3d, 0x42a, 0xa, 0x3d, 0x3, 0x3d, 
    0x3, 0x3d, 0x5, 0x3d, 0x42e, 0xa, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x5, 0x3d, 
    0x432, 0xa, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x5, 0x3d, 0x436, 0xa, 0x3d, 
    0x3, 0x3d, 0x3, 0x3d, 0x5, 0x3d, 0x43a, 0xa, 0x3d, 0x7, 0x3d, 0x43c, 
    0xa, 0x3d, 0xc, 0x3d, 0xe, 0x3d, 0x43f, 0xb, 0x3d, 0x5, 0x3d, 0x441, 
    0xa, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3e, 0x3, 0x3e, 0x5, 0x3e, 0x447, 
    0xa, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x5, 0x3e, 0x44b, 0xa, 0x3e, 0x3, 0x3e, 
    0x3, 0x3e, 0x5, 0x3e, 0x44f, 0xa, 0x3e, 0x3, 0x3e, 0x5, 0x3e, 0x452, 
    0xa, 0x3e, 0x3, 0x3e, 0x7, 0x3e, 0x455, 0xa, 0x3e, 0xc, 0x3e, 0xe, 0x3e, 
    0x458, 0xb, 0x3e, 0x3, 0x3f, 0x3, 0x3f, 0x5, 0x3f, 0x45c, 0xa, 0x3f, 
    0x3, 0x3f, 0x7, 0x3f, 0x45f, 0xa, 0x3f, 0xc, 0x3f, 0xe, 0x3f, 0x462, 
    0xb, 0x3f, 0x3, 0x40, 0x3, 0x40, 0x5, 0x40, 0x466, 0xa, 0x40, 0x3, 0x40, 
    0x3, 0x40, 0x3, 0x41, 0x3, 0x41, 0x5, 0x41, 0x46c, 0xa, 0x41, 0x3, 0x41, 
    0x5, 0x41, 0x46f, 0xa, 0x41, 0x3, 0x41, 0x5, 0x41, 0x472, 0xa, 0x41, 
    0x3, 0x41, 0x3, 0x41, 0x5, 0x41, 0x476, 0xa, 0x41, 0x3, 0x41, 0x3, 0x41, 
    0x5, 0x41, 0x47a, 0xa, 0x41, 0x3, 0x41, 0x3, 0x41, 0x3, 0x42, 0x3, 0x42, 
    0x3, 0x43, 0x3, 0x43, 0x3, 0x44, 0x3, 0x44, 0x3, 0x45, 0x3, 0x45, 0x3, 
    0x45, 0x3, 0x45, 0x3, 0x45, 0x7, 0x45, 0x489, 0xa, 0x45, 0xc, 0x45, 
    0xe, 0x45, 0x48c, 0xb, 0x45, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 
    0x3, 0x46, 0x7, 0x46, 0x493, 0xa, 0x46, 0xc, 0x46, 0xe, 0x46, 0x496, 
    0xb, 0x46, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x7, 
    0x47, 0x49d, 0xa, 0x47, 0xc, 0x47, 0xe, 0x47, 0x4a0, 0xb, 0x47, 0x3, 
    0x48, 0x3, 0x48, 0x5, 0x48, 0x4a4, 0xa, 0x48, 0x5, 0x48, 0x4a6, 0xa, 
    0x48, 0x3, 0x48, 0x3, 0x48, 0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x4ac, 
    0xa, 0x49, 0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x4b0, 0xa, 0x49, 0x3, 0x49, 
    0x3, 0x49, 0x5, 0x49, 0x4b4, 0xa, 0x49, 0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 
    0x4b8, 0xa, 0x49, 0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x4bc, 0xa, 0x49, 
    0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 0x5, 
    0x49, 0x4c4, 0xa, 0x49, 0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x4c8, 0xa, 
    0x49, 0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x4cc, 0xa, 0x49, 0x3, 0x49, 
    0x3, 0x49, 0x5, 0x49, 0x4d0, 0xa, 0x49, 0x3, 0x49, 0x3, 0x49, 0x6, 0x49, 
    0x4d4, 0xa, 0x49, 0xd, 0x49, 0xe, 0x49, 0x4d5, 0x3, 0x49, 0x3, 0x49, 
    0x5, 0x49, 0x4da, 0xa, 0x49, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4b, 0x3, 0x4b, 
    0x5, 0x4b, 0x4e0, 0xa, 0x4b, 0x3, 0x4b, 0x3, 0x4b, 0x5, 0x4b, 0x4e4, 
    0xa, 0x4b, 0x3, 0x4b, 0x7, 0x4b, 0x4e7, 0xa, 0x4b, 0xc, 0x4b, 0xe, 0x4b, 
    0x4ea, 0xb, 0x4b, 0x3, 0x4c, 0x3, 0x4c, 0x5, 0x4c, 0x4ee, 0xa, 0x4c, 
    0x3, 0x4c, 0x3, 0x4c, 0x5, 0x4c, 0x4f2, 0xa, 0x4c, 0x3, 0x4c, 0x7, 0x4c, 
    0x4f5, 0xa, 0x4c, 0xc, 0x4c, 0xe, 0x4c, 0x4f8, 0xb, 0x4c, 0x3, 0x4d, 
    0x3, 0x4d, 0x5, 0x4d, 0x4fc, 0xa, 0x4d, 0x3, 0x4d, 0x3, 0x4d, 0x5, 0x4d, 
    0x500, 0xa, 0x4d, 0x3, 0x4d, 0x3, 0x4d, 0x7, 0x4d, 0x504, 0xa, 0x4d, 
    0xc, 0x4d, 0xe, 0x4d, 0x507, 0xb, 0x4d, 0x3, 0x4e, 0x3, 0x4e, 0x3, 0x4f, 
    0x3, 0x4f, 0x5, 0x4f, 0x50d, 0xa, 0x4f, 0x3, 0x4f, 0x3, 0x4f, 0x5, 0x4f, 
    0x511, 0xa, 0x4f, 0x3, 0x4f, 0x3, 0x4f, 0x7, 0x4f, 0x515, 0xa, 0x4f, 
    0xc, 0x4f, 0xe, 0x4f, 0x518, 0xb, 0x4f, 0x3, 0x50, 0x3, 0x50, 0x3, 0x51, 
    0x3, 0x51, 0x5, 0x51, 0x51e, 0xa, 0x51, 0x3, 0x51, 0x3, 0x51, 0x5, 0x51, 
    0x522, 0xa, 0x51, 0x3, 0x51, 0x3, 0x51, 0x7, 0x51, 0x526, 0xa, 0x51, 
    0xc, 0x51, 0xe, 0x51, 0x529, 0xb, 0x51, 0x3, 0x52, 0x3, 0x52, 0x3, 0x53, 
    0x3, 0x53, 0x5, 0x53, 0x52f, 0xa, 0x53, 0x3, 0x53, 0x3, 0x53, 0x5, 0x53, 
    0x533, 0xa, 0x53, 0x3, 0x53, 0x7, 0x53, 0x536, 0xa, 0x53, 0xc, 0x53, 
    0xe, 0x53, 0x539, 0xb, 0x53, 0x3, 0x54, 0x3, 0x54, 0x5, 0x54, 0x53d, 
    0xa, 0x54, 0x5, 0x54, 0x53f, 0xa, 0x54, 0x3, 0x54, 0x3, 0x54, 0x5, 0x54, 
    0x543, 0xa, 0x54, 0x3, 0x54, 0x5, 0x54, 0x546, 0xa, 0x54, 0x3, 0x55, 
    0x3, 0x55, 0x3, 0x55, 0x3, 0x55, 0x5, 0x55, 0x54c, 0xa, 0x55, 0x3, 0x56, 
    0x3, 0x56, 0x5, 0x56, 0x550, 0xa, 0x56, 0x3, 0x56, 0x5, 0x56, 0x553, 
    0xa, 0x56, 0x3, 0x57, 0x5, 0x57, 0x556, 0xa, 0x57, 0x3, 0x57, 0x3, 0x57, 
    0x3, 0x57, 0x3, 0x57, 0x3, 0x58, 0x5, 0x58, 0x55d, 0xa, 0x58, 0x3, 0x58, 
    0x3, 0x58, 0x5, 0x58, 0x561, 0xa, 0x58, 0x3, 0x58, 0x3, 0x58, 0x5, 0x58, 
    0x565, 0xa, 0x58, 0x3, 0x58, 0x3, 0x58, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 
    0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 
    0x59, 0x3, 0x59, 0x5, 0x59, 0x574, 0xa, 0x59, 0x3, 0x59, 0x5, 0x59, 
    0x577, 0xa, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x5a, 0x5, 0x5a, 0x57c, 
    0xa, 0x5a, 0x3, 0x5a, 0x3, 0x5a, 0x3, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 0x3, 
    0x5b, 0x3, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 
    0x5, 0x5b, 0x58a, 0xa, 0x5b, 0x3, 0x5c, 0x3, 0x5c, 0x5, 0x5c, 0x58e, 
    0xa, 0x5c, 0x3, 0x5c, 0x5, 0x5c, 0x591, 0xa, 0x5c, 0x3, 0x5d, 0x3, 0x5d, 
    0x3, 0x5d, 0x3, 0x5d, 0x3, 0x5d, 0x3, 0x5d, 0x3, 0x5d, 0x5, 0x5d, 0x59a, 
    0xa, 0x5d, 0x3, 0x5e, 0x3, 0x5e, 0x3, 0x5e, 0x3, 0x5e, 0x3, 0x5e, 0x3, 
    0x5e, 0x5, 0x5e, 0x5a2, 0xa, 0x5e, 0x3, 0x5f, 0x3, 0x5f, 0x3, 0x60, 
    0x3, 0x60, 0x5, 0x60, 0x5a8, 0xa, 0x60, 0x3, 0x60, 0x3, 0x60, 0x5, 0x60, 
    0x5ac, 0xa, 0x60, 0x3, 0x60, 0x3, 0x60, 0x5, 0x60, 0x5b0, 0xa, 0x60, 
    0x3, 0x60, 0x3, 0x60, 0x5, 0x60, 0x5b4, 0xa, 0x60, 0x7, 0x60, 0x5b6, 
    0xa, 0x60, 0xc, 0x60, 0xe, 0x60, 0x5b9, 0xb, 0x60, 0x5, 0x60, 0x5bb, 
    0xa, 0x60, 0x3, 0x60, 0x3, 0x60, 0x3, 0x61, 0x3, 0x61, 0x5, 0x61, 0x5c1, 
    0xa, 0x61, 0x3, 0x61, 0x3, 0x61, 0x5, 0x61, 0x5c5, 0xa, 0x61, 0x3, 0x61, 
    0x3, 0x61, 0x5, 0x61, 0x5c9, 0xa, 0x61, 0x3, 0x61, 0x3, 0x61, 0x5, 0x61, 
    0x5cd, 0xa, 0x61, 0x7, 0x61, 0x5cf, 0xa, 0x61, 0xc, 0x61, 0xe, 0x61, 
    0x5d2, 0xb, 0x61, 0x3, 0x61, 0x3, 0x61, 0x3, 0x62, 0x3, 0x62, 0x5, 0x62, 
    0x5d8, 0xa, 0x62, 0x3, 0x62, 0x3, 0x62, 0x5, 0x62, 0x5dc, 0xa, 0x62, 
    0x3, 0x62, 0x3, 0x62, 0x3, 0x63, 0x3, 0x63, 0x5, 0x63, 0x5e2, 0xa, 0x63, 
    0x3, 0x63, 0x3, 0x63, 0x5, 0x63, 0x5e6, 0xa, 0x63, 0x3, 0x63, 0x3, 0x63, 
    0x3, 0x64, 0x3, 0x64, 0x5, 0x64, 0x5ec, 0xa, 0x64, 0x3, 0x64, 0x3, 0x64, 
    0x5, 0x64, 0x5f0, 0xa, 0x64, 0x3, 0x64, 0x3, 0x64, 0x5, 0x64, 0x5f4, 
    0xa, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x5, 0x64, 0x5fa, 
    0xa, 0x64, 0x3, 0x64, 0x3, 0x64, 0x5, 0x64, 0x5fe, 0xa, 0x64, 0x3, 0x64, 
    0x3, 0x64, 0x5, 0x64, 0x602, 0xa, 0x64, 0x5, 0x64, 0x604, 0xa, 0x64, 
    0x3, 0x64, 0x3, 0x64, 0x5, 0x64, 0x608, 0xa, 0x64, 0x3, 0x64, 0x3, 0x64, 
    0x5, 0x64, 0x60c, 0xa, 0x64, 0x3, 0x64, 0x3, 0x64, 0x5, 0x64, 0x610, 
    0xa, 0x64, 0x7, 0x64, 0x612, 0xa, 0x64, 0xc, 0x64, 0xe, 0x64, 0x615, 
    0xb, 0x64, 0x5, 0x64, 0x617, 0xa, 0x64, 0x3, 0x64, 0x3, 0x64, 0x5, 0x64, 
    0x61b, 0xa, 0x64, 0x3, 0x65, 0x3, 0x65, 0x3, 0x66, 0x3, 0x66, 0x5, 0x66, 
    0x621, 0xa, 0x66, 0x3, 0x66, 0x3, 0x66, 0x5, 0x66, 0x625, 0xa, 0x66, 
    0x3, 0x66, 0x3, 0x66, 0x5, 0x66, 0x629, 0xa, 0x66, 0x3, 0x66, 0x3, 0x66, 
    0x5, 0x66, 0x62d, 0xa, 0x66, 0x3, 0x66, 0x5, 0x66, 0x630, 0xa, 0x66, 
    0x3, 0x66, 0x5, 0x66, 0x633, 0xa, 0x66, 0x3, 0x66, 0x3, 0x66, 0x3, 0x67, 
    0x3, 0x67, 0x5, 0x67, 0x639, 0xa, 0x67, 0x3, 0x67, 0x3, 0x67, 0x3, 0x68, 
    0x3, 0x68, 0x5, 0x68, 0x63f, 0xa, 0x68, 0x3, 0x68, 0x6, 0x68, 0x642, 
    0xa, 0x68, 0xd, 0x68, 0xe, 0x68, 0x643, 0x3, 0x68, 0x3, 0x68, 0x5, 0x68, 
    0x648, 0xa, 0x68, 0x3, 0x68, 0x3, 0x68, 0x5, 0x68, 0x64c, 0xa, 0x68, 
    0x3, 0x68, 0x6, 0x68, 0x64f, 0xa, 0x68, 0xd, 0x68, 0xe, 0x68, 0x650, 
    0x5, 0x68, 0x653, 0xa, 0x68, 0x3, 0x68, 0x5, 0x68, 0x656, 0xa, 0x68, 
    0x3, 0x68, 0x3, 0x68, 0x5, 0x68, 0x65a, 0xa, 0x68, 0x3, 0x68, 0x5, 0x68, 
    0x65d, 0xa, 0x68, 0x3, 0x68, 0x5, 0x68, 0x660, 0xa, 0x68, 0x3, 0x68, 
    0x3, 0x68, 0x3, 0x69, 0x3, 0x69, 0x5, 0x69, 0x666, 0xa, 0x69, 0x3, 0x69, 
    0x3, 0x69, 0x5, 0x69, 0x66a, 0xa, 0x69, 0x3, 0x69, 0x3, 0x69, 0x5, 0x69, 
    0x66e, 0xa, 0x69, 0x3, 0x69, 0x3, 0x69, 0x3, 0x6a, 0x3, 0x6a, 0x3, 0x6b, 
    0x3, 0x6b, 0x5, 0x6b, 0x676, 0xa, 0x6b, 0x3, 0x6c, 0x3, 0x6c, 0x3, 0x6c, 
    0x5, 0x6c, 0x67b, 0xa, 0x6c, 0x3, 0x6d, 0x3, 0x6d, 0x5, 0x6d, 0x67f, 
    0xa, 0x6d, 0x3, 0x6d, 0x3, 0x6d, 0x3, 0x6e, 0x3, 0x6e, 0x3, 0x6f, 0x3, 
    0x6f, 0x3, 0x70, 0x3, 0x70, 0x3, 0x71, 0x3, 0x71, 0x3, 0x72, 0x3, 0x72, 
    0x3, 0x72, 0x3, 0x72, 0x5, 0x72, 0x68f, 0xa, 0x72, 0x3, 0x73, 0x3, 0x73, 
    0x3, 0x74, 0x3, 0x74, 0x3, 0x75, 0x3, 0x75, 0x3, 0x75, 0x2, 0x2, 0x76, 
    0x2, 0x4, 0x6, 0x8, 0xa, 0xc, 0xe, 0x10, 0x12, 0x14, 0x16, 0x18, 0x1a, 
    0x1c, 0x1e, 0x20, 0x22, 0x24, 0x26, 0x28, 0x2a, 0x2c, 0x2e, 0x30, 0x32, 
    0x34, 0x36, 0x38, 0x3a, 0x3c, 0x3e, 0x40, 0x42, 0x44, 0x46, 0x48, 0x4a, 
    0x4c, 0x4e, 0x50, 0x52, 0x54, 0x56, 0x58, 0x5a, 0x5c, 0x5e, 0x60, 0x62, 
    0x64, 0x66, 0x68, 0x6a, 0x6c, 0x6e, 0x70, 0x72, 0x74, 0x76, 0x78, 0x7a, 
    0x7c, 0x7e, 0x80, 0x82, 0x84, 0x86, 0x88, 0x8a, 0x8c, 0x8e, 0x90, 0x92, 
    0x94, 0x96, 0x98, 0x9a, 0x9c, 0x9e, 0xa0, 0xa2, 0xa4, 0xa6, 0xa8, 0xaa, 
    0xac, 0xae, 0xb0, 0xb2, 0xb4, 0xb6, 0xb8, 0xba, 0xbc, 0xbe, 0xc0, 0xc2, 
    0xc4, 0xc6, 0xc8, 0xca, 0xcc, 0xce, 0xd0, 0xd2, 0xd4, 0xd6, 0xd8, 0xda, 
    0xdc, 0xde, 0xe0, 0xe2, 0xe4, 0xe6, 0xe8, 0x2, 0xb, 0x3, 0x2, 0x54, 
    0x57, 0x4, 0x2, 0x9, 0x9, 0xf, 0x13, 0x3, 0x2, 0x15, 0x16, 0x4, 0x2, 
    0x17, 0x17, 0x5f, 0x5f, 0x4, 0x2, 0x18, 0x19, 0x4e, 0x4e, 0x3, 0x2, 
    0x66, 0x67, 0x4, 0x2, 0x10, 0x10, 0x1e, 0x21, 0x4, 0x2, 0x12, 0x12, 
    0x22, 0x25, 0x4, 0x2, 0x26, 0x30, 0x5f, 0x5f, 0x2, 0x760, 0x2, 0xeb, 
    0x3, 0x2, 0x2, 0x2, 0x4, 0x104, 0x3, 0x2, 0x2, 0x2, 0x6, 0x11a, 0x3, 
    0x2, 0x2, 0x2, 0x8, 0x159, 0x3, 0x2, 0x2, 0x2, 0xa, 0x15b, 0x3, 0x2, 
    0x2, 0x2, 0xc, 0x169, 0x3, 0x2, 0x2, 0x2, 0xe, 0x177, 0x3, 0x2, 0x2, 
    0x2, 0x10, 0x179, 0x3, 0x2, 0x2, 0x2, 0x12, 0x196, 0x3, 0x2, 0x2, 0x2, 
    0x14, 0x1c4, 0x3, 0x2, 0x2, 0x2, 0x16, 0x1ca, 0x3, 0x2, 0x2, 0x2, 0x18, 
    0x1d6, 0x3, 0x2, 0x2, 0x2, 0x1a, 0x1d8, 0x3, 0x2, 0x2, 0x2, 0x1c, 0x1e3, 
    0x3, 0x2, 0x2, 0x2, 0x1e, 0x1e7, 0x3, 0x2, 0x2, 0x2, 0x20, 0x1ed, 0x3, 
    0x2, 0x2, 0x2, 0x22, 0x1f5, 0x3, 0x2, 0x2, 0x2, 0x24, 0x203, 0x3, 0x2, 
    0x2, 0x2, 0x26, 0x207, 0x3, 0x2, 0x2, 0x2, 0x28, 0x229, 0x3, 0x2, 0x2, 
    0x2, 0x2a, 0x22b, 0x3, 0x2, 0x2, 0x2, 0x2c, 0x232, 0x3, 0x2, 0x2, 0x2, 
    0x2e, 0x23a, 0x3, 0x2, 0x2, 0x2, 0x30, 0x23c, 0x3, 0x2, 0x2, 0x2, 0x32, 
    0x23e, 0x3, 0x2, 0x2, 0x2, 0x34, 0x240, 0x3, 0x2, 0x2, 0x2, 0x36, 0x242, 
    0x3, 0x2, 0x2, 0x2, 0x38, 0x259, 0x3, 0x2, 0x2, 0x2, 0x3a, 0x267, 0x3, 
    0x2, 0x2, 0x2, 0x3c, 0x26b, 0x3, 0x2, 0x2, 0x2, 0x3e, 0x29a, 0x3, 0x2, 
    0x2, 0x2, 0x40, 0x2a0, 0x3, 0x2, 0x2, 0x2, 0x42, 0x2ac, 0x3, 0x2, 0x2, 
    0x2, 0x44, 0x2bd, 0x3, 0x2, 0x2, 0x2, 0x46, 0x2c1, 0x3, 0x2, 0x2, 0x2, 
    0x48, 0x2c5, 0x3, 0x2, 0x2, 0x2, 0x4a, 0x2d2, 0x3, 0x2, 0x2, 0x2, 0x4c, 
    0x2dc, 0x3, 0x2, 0x2, 0x2, 0x4e, 0x2e2, 0x3, 0x2, 0x2, 0x2, 0x50, 0x2f4, 
    0x3, 0x2, 0x2, 0x2, 0x52, 0x2fe, 0x3, 0x2, 0x2, 0x2, 0x54, 0x310, 0x3, 
    0x2, 0x2, 0x2, 0x56, 0x318, 0x3, 0x2, 0x2, 0x2, 0x58, 0x31f, 0x3, 0x2, 
    0x2, 0x2, 0x5a, 0x34b, 0x3, 0x2, 0x2, 0x2, 0x5c, 0x354, 0x3, 0x2, 0x2, 
    0x2, 0x5e, 0x356, 0x3, 0x2, 0x2, 0x2, 0x60, 0x365, 0x3, 0x2, 0x2, 0x2, 
    0x62, 0x369, 0x3, 0x2, 0x2, 0x2, 0x64, 0x36d, 0x3, 0x2, 0x2, 0x2, 0x66, 
    0x374, 0x3, 0x2, 0x2, 0x2, 0x68, 0x378, 0x3, 0x2, 0x2, 0x2, 0x6a, 0x386, 
    0x3, 0x2, 0x2, 0x2, 0x6c, 0x388, 0x3, 0x2, 0x2, 0x2, 0x6e, 0x398, 0x3, 
    0x2, 0x2, 0x2, 0x70, 0x3c7, 0x3, 0x2, 0x2, 0x2, 0x72, 0x3c9, 0x3, 0x2, 
    0x2, 0x2, 0x74, 0x3fb, 0x3, 0x2, 0x2, 0x2, 0x76, 0x3fd, 0x3, 0x2, 0x2, 
    0x2, 0x78, 0x41b, 0x3, 0x2, 0x2, 0x2, 0x7a, 0x444, 0x3, 0x2, 0x2, 0x2, 
    0x7c, 0x459, 0x3, 0x2, 0x2, 0x2, 0x7e, 0x463, 0x3, 0x2, 0x2, 0x2, 0x80, 
    0x469, 0x3, 0x2, 0x2, 0x2, 0x82, 0x47d, 0x3, 0x2, 0x2, 0x2, 0x84, 0x47f, 
    0x3, 0x2, 0x2, 0x2, 0x86, 0x481, 0x3, 0x2, 0x2, 0x2, 0x88, 0x483, 0x3, 
    0x2, 0x2, 0x2, 0x8a, 0x48d, 0x3, 0x2, 0x2, 0x2, 0x8c, 0x497, 0x3, 0x2, 
    0x2, 0x2, 0x8e, 0x4a5, 0x3, 0x2, 0x2, 0x2, 0x90, 0x4d9, 0x3, 0x2, 0x2, 
    0x2, 0x92, 0x4db, 0x3, 0x2, 0x2, 0x2, 0x94, 0x4dd, 0x3, 0x2, 0x2, 0x2, 
    0x96, 0x4eb, 0x3, 0x2, 0x2, 0x2, 0x98, 0x4f9, 0x3, 0x2, 0x2, 0x2, 0x9a, 
    0x508, 0x3, 0x2, 0x2, 0x2, 0x9c, 0x50a, 0x3, 0x2, 0x2, 0x2, 0x9e, 0x519, 
    0x3, 0x2, 0x2, 0x2, 0xa0, 0x51b, 0x3, 0x2, 0x2, 0x2, 0xa2, 0x52a, 0x3, 
    0x2, 0x2, 0x2, 0xa4, 0x52c, 0x3, 0x2, 0x2, 0x2, 0xa6, 0x53e, 0x3, 0x2, 
    0x2, 0x2, 0xa8, 0x547, 0x3, 0x2, 0x2, 0x2, 0xaa, 0x54f, 0x3, 0x2, 0x2, 
    0x2, 0xac, 0x555, 0x3, 0x2, 0x2, 0x2, 0xae, 0x55c, 0x3, 0x2, 0x2, 0x2, 
    0xb0, 0x573, 0x3, 0x2, 0x2, 0x2, 0xb2, 0x57b, 0x3, 0x2, 0x2, 0x2, 0xb4, 
    0x589, 0x3, 0x2, 0x2, 0x2, 0xb6, 0x58b, 0x3, 0x2, 0x2, 0x2, 0xb8, 0x599, 
    0x3, 0x2, 0x2, 0x2, 0xba, 0x5a1, 0x3, 0x2, 0x2, 0x2, 0xbc, 0x5a3, 0x3, 
    0x2, 0x2, 0x2, 0xbe, 0x5a5, 0x3, 0x2, 0x2, 0x2, 0xc0, 0x5be, 0x3, 0x2, 
    0x2, 0x2, 0xc2, 0x5d5, 0x3, 0x2, 0x2, 0x2, 0xc4, 0x5df, 0x3, 0x2, 0x2, 
    0x2, 0xc6, 0x61a, 0x3, 0x2, 0x2, 0x2, 0xc8, 0x61c, 0x3, 0x2, 0x2, 0x2, 
    0xca, 0x61e, 0x3, 0x2, 0x2, 0x2, 0xcc, 0x636, 0x3, 0x2, 0x2, 0x2, 0xce, 
    0x652, 0x3, 0x2, 0x2, 0x2, 0xd0, 0x663, 0x3, 0x2, 0x2, 0x2, 0xd2, 0x671, 
    0x3, 0x2, 0x2, 0x2, 0xd4, 0x675, 0x3, 0x2, 0x2, 0x2, 0xd6, 0x677, 0x3, 
    0x2, 0x2, 0x2, 0xd8, 0x67c, 0x3, 0x2, 0x2, 0x2, 0xda, 0x682, 0x3, 0x2, 
    0x2, 0x2, 0xdc, 0x684, 0x3, 0x2, 0x2, 0x2, 0xde, 0x686, 0x3, 0x2, 0x2, 
    0x2, 0xe0, 0x688, 0x3, 0x2, 0x2, 0x2, 0xe2, 0x68e, 0x3, 0x2, 0x2, 0x2, 
    0xe4, 0x690, 0x3, 0x2, 0x2, 0x2, 0xe6, 0x692, 0x3, 0x2, 0x2, 0x2, 0xe8, 
    0x694, 0x3, 0x2, 0x2, 0x2, 0xea, 0xec, 0x7, 0x7c, 0x2, 0x2, 0xeb, 0xea, 
    0x3, 0x2, 0x2, 0x2, 0xeb, 0xec, 0x3, 0x2, 0x2, 0x2, 0xec, 0xee, 0x3, 
    0x2, 0x2, 0x2, 0xed, 0xef, 0x5, 0x2e, 0x18, 0x2, 0xee, 0xed, 0x3, 0x2, 
    0x2, 0x2, 0xee, 0xef, 0x3, 0x2, 0x2, 0x2, 0xef, 0xf1, 0x3, 0x2, 0x2, 
    0x2, 0xf0, 0xf2, 0x7, 0x7c, 0x2, 0x2, 0xf1, 0xf0, 0x3, 0x2, 0x2, 0x2, 
    0xf1, 0xf2, 0x3, 0x2, 0x2, 0x2, 0xf2, 0xf7, 0x3, 0x2, 0x2, 0x2, 0xf3, 
    0xf8, 0x5, 0x34, 0x1b, 0x2, 0xf4, 0xf8, 0x5, 0xe, 0x8, 0x2, 0xf5, 0xf8, 
    0x5, 0x6, 0x4, 0x2, 0xf6, 0xf8, 0x5, 0x4, 0x3, 0x2, 0xf7, 0xf3, 0x3, 
    0x2, 0x2, 0x2, 0xf7, 0xf4, 0x3, 0x2, 0x2, 0x2, 0xf7, 0xf5, 0x3, 0x2, 
    0x2, 0x2, 0xf7, 0xf6, 0x3, 0x2, 0x2, 0x2, 0xf8, 0xfd, 0x3, 0x2, 0x2, 
    0x2, 0xf9, 0xfb, 0x7, 0x7c, 0x2, 0x2, 0xfa, 0xf9, 0x3, 0x2, 0x2, 0x2, 
    0xfa, 0xfb, 0x3, 0x2, 0x2, 0x2, 0xfb, 0xfc, 0x3, 0x2, 0x2, 0x2, 0xfc, 
    0xfe, 0x7, 0x3, 0x2, 0x2, 0xfd, 0xfa, 0x3, 0x2, 0x2, 0x2, 0xfd, 0xfe, 
    0x3, 0x2, 0x2, 0x2, 0xfe, 0x100, 0x3, 0x2, 0x2, 0x2, 0xff, 0x101, 0x7, 
    0x7c, 0x2, 0x2, 0x100, 0xff, 0x3, 0x2, 0x2, 0x2, 0x100, 0x101, 0x3, 
    0x2, 0x2, 0x2, 0x101, 0x102, 0x3, 0x2, 0x2, 0x2, 0x102, 0x103, 0x7, 
    0x2, 0x2, 0x3, 0x103, 0x3, 0x3, 0x2, 0x2, 0x2, 0x104, 0x105, 0x7, 0x32, 
    0x2, 0x2, 0x105, 0x106, 0x7, 0x7c, 0x2, 0x2, 0x106, 0x107, 0x5, 0xe0, 
    0x71, 0x2, 0x107, 0x108, 0x7, 0x7c, 0x2, 0x2, 0x108, 0x109, 0x7, 0x33, 
    0x2, 0x2, 0x109, 0x10a, 0x7, 0x7c, 0x2, 0x2, 0x10a, 0x118, 0x5, 0x8, 
    0x5, 0x2, 0x10b, 0x10d, 0x7, 0x7c, 0x2, 0x2, 0x10c, 0x10b, 0x3, 0x2, 
    0x2, 0x2, 0x10c, 0x10d, 0x3, 0x2, 0x2, 0x2, 0x10d, 0x10e, 0x3, 0x2, 
    0x2, 0x2, 0x10e, 0x110, 0x7, 0x4, 0x2, 0x2, 0x10f, 0x111, 0x7, 0x7c, 
    0x2, 0x2, 0x110, 0x10f, 0x3, 0x2, 0x2, 0x2, 0x110, 0x111, 0x3, 0x2, 
    0x2, 0x2, 0x111, 0x112, 0x3, 0x2, 0x2, 0x2, 0x112, 0x114, 0x5, 0xa, 
    0x6, 0x2, 0x113, 0x115, 0x7, 0x7c, 0x2, 0x2, 0x114, 0x113, 0x3, 0x2, 
    0x2, 0x2, 0x114, 0x115, 0x3, 0x2, 0x2, 0x2, 0x115, 0x116, 0x3, 0x2, 
    0x2, 0x2, 0x116, 0x117, 0x7, 0x5, 0x2, 0x2, 0x117, 0x119, 0x3, 0x2, 
    0x2, 0x2, 0x118, 0x10c, 0x3, 0x2, 0x2, 0x2, 0x118, 0x119, 0x3, 0x2, 
    0x2, 0x2, 0x119, 0x5, 0x3, 0x2, 0x2, 0x2, 0x11a, 0x11b, 0x7, 0x32, 0x2, 
    0x2, 0x11b, 0x11c, 0x7, 0x7c, 0x2, 0x2, 0x11c, 0x11d, 0x5, 0xe0, 0x71, 
    0x2, 0x11d, 0x11e, 0x7, 0x7c, 0x2, 0x2, 0x11e, 0x11f, 0x7, 0x33, 0x2, 
    0x2, 0x11f, 0x120, 0x7, 0x7c, 0x2, 0x2, 0x120, 0x122, 0x7, 0x4, 0x2, 
    0x2, 0x121, 0x123, 0x7, 0x7c, 0x2, 0x2, 0x122, 0x121, 0x3, 0x2, 0x2, 
    0x2, 0x122, 0x123, 0x3, 0x2, 0x2, 0x2, 0x123, 0x124, 0x3, 0x2, 0x2, 
    0x2, 0x124, 0x12f, 0x7, 0x6e, 0x2, 0x2, 0x125, 0x127, 0x7, 0x7c, 0x2, 
    0x2, 0x126, 0x125, 0x3, 0x2, 0x2, 0x2, 0x126, 0x127, 0x3, 0x2, 0x2, 
    0x2, 0x127, 0x128, 0x3, 0x2, 0x2, 0x2, 0x128, 0x12a, 0x7, 0x6, 0x2, 
    0x2, 0x129, 0x12b, 0x7, 0x7c, 0x2, 0x2, 0x12a, 0x129, 0x3, 0x2, 0x2, 
    0x2, 0x12a, 0x12b, 0x3, 0x2, 0x2, 0x2, 0x12b, 0x12c, 0x3, 0x2, 0x2, 
    0x2, 0x12c, 0x12e, 0x7, 0x6e, 0x2, 0x2, 0x12d, 0x126, 0x3, 0x2, 0x2, 
    0x2, 0x12e, 0x131, 0x3, 0x2, 0x2, 0x2, 0x12f, 0x12d, 0x3, 0x2, 0x2, 
    0x2, 0x12f, 0x130, 0x3, 0x2, 0x2, 0x2, 0x130, 0x132, 0x3, 0x2, 0x2, 
    0x2, 0x131, 0x12f, 0x3, 0x2, 0x2, 0x2, 0x132, 0x133, 0x7, 0x5, 0x2, 
    0x2, 0x133, 0x134, 0x7, 0x7c, 0x2, 0x2, 0x134, 0x135, 0x7, 0x51, 0x2, 
    0x2, 0x135, 0x136, 0x7, 0x7c, 0x2, 0x2, 0x136, 0x137, 0x7, 0x35, 0x2, 
    0x2, 0x137, 0x7, 0x3, 0x2, 0x2, 0x2, 0x138, 0x13a, 0x7, 0x7, 0x2, 0x2, 
    0x139, 0x13b, 0x7, 0x7c, 0x2, 0x2, 0x13a, 0x139, 0x3, 0x2, 0x2, 0x2, 
    0x13a, 0x13b, 0x3, 0x2, 0x2, 0x2, 0x13b, 0x13c, 0x3, 0x2, 0x2, 0x2, 
    0x13c, 0x147, 0x7, 0x6e, 0x2, 0x2, 0x13d, 0x13f, 0x7, 0x7c, 0x2, 0x2, 
    0x13e, 0x13d, 0x3, 0x2, 0x2, 0x2, 0x13e, 0x13f, 0x3, 0x2, 0x2, 0x2, 
    0x13f, 0x140, 0x3, 0x2, 0x2, 0x2, 0x140, 0x142, 0x7, 0x6, 0x2, 0x2, 
    0x141, 0x143, 0x7, 0x7c, 0x2, 0x2, 0x142, 0x141, 0x3, 0x2, 0x2, 0x2, 
    0x142, 0x143, 0x3, 0x2, 0x2, 0x2, 0x143, 0x144, 0x3, 0x2, 0x2, 0x2, 
    0x144, 0x146, 0x7, 0x6e, 0x2, 0x2, 0x145, 0x13e, 0x3, 0x2, 0x2, 0x2, 
    0x146, 0x149, 0x3, 0x2, 0x2, 0x2, 0x147, 0x145, 0x3, 0x2, 0x2, 0x2, 
    0x147, 0x148, 0x3, 0x2, 0x2, 0x2, 0x148, 0x14a, 0x3, 0x2, 0x2, 0x2, 
    0x149, 0x147, 0x3, 0x2, 0x2, 0x2, 0x14a, 0x15a, 0x7, 0x8, 0x2, 0x2, 
    0x14b, 0x15a, 0x7, 0x6e, 0x2, 0x2, 0x14c, 0x14e, 0x7, 0x31, 0x2, 0x2, 
    0x14d, 0x14f, 0x7, 0x7c, 0x2, 0x2, 0x14e, 0x14d, 0x3, 0x2, 0x2, 0x2, 
    0x14e, 0x14f, 0x3, 0x2, 0x2, 0x2, 0x14f, 0x150, 0x3, 0x2, 0x2, 0x2, 
    0x150, 0x152, 0x7, 0x4, 0x2, 0x2, 0x151, 0x153, 0x7, 0x7c, 0x2, 0x2, 
    0x152, 0x151, 0x3, 0x2, 0x2, 0x2, 0x152, 0x153, 0x3, 0x2, 0x2, 0x2, 
    0x153, 0x154, 0x3, 0x2, 0x2, 0x2, 0x154, 0x156, 0x7, 0x6e, 0x2, 0x2, 
    0x155, 0x157, 0x7, 0x7c, 0x2, 0x2, 0x156, 0x155, 0x3, 0x2, 0x2, 0x2, 
    0x156, 0x157, 0x3, 0x2, 0x2, 0x2, 0x157, 0x158, 0x3, 0x2, 0x2, 0x2, 
    0x158, 0x15a, 0x7, 0x5, 0x2, 0x2, 0x159, 0x138, 0x3, 0x2, 0x2, 0x2, 
    0x159, 0x14b, 0x3, 0x2, 0x2, 0x2, 0x159, 0x14c, 0x3, 0x2, 0x2, 0x2, 
    0x15a, 0x9, 0x3, 0x2, 0x2, 0x2, 0x15b, 0x166, 0x5, 0xc, 0x7, 0x2, 0x15c, 
    0x15e, 0x7, 0x7c, 0x2, 0x2, 0x15d, 0x15c, 0x3, 0x2, 0x2, 0x2, 0x15d, 
    0x15e, 0x3, 0x2, 0x2, 0x2, 0x15e, 0x15f, 0x3, 0x2, 0x2, 0x2, 0x15f, 
    0x161, 0x7, 0x6, 0x2, 0x2, 0x160, 0x162, 0x7, 0x7c, 0x2, 0x2, 0x161, 
    0x160, 0x3, 0x2, 0x2, 0x2, 0x161, 0x162, 0x3, 0x2, 0x2, 0x2, 0x162, 
    0x163, 0x3, 0x2, 0x2, 0x2, 0x163, 0x165, 0x5, 0xc, 0x7, 0x2, 0x164, 
    0x15d, 0x3, 0x2, 0x2, 0x2, 0x165, 0x168, 0x3, 0x2, 0x2, 0x2, 0x166, 
    0x164, 0x3, 0x2, 0x2, 0x2, 0x166, 0x167, 0x3, 0x2, 0x2, 0x2, 0x167, 
    0xb, 0x3, 0x2, 0x2, 0x2, 0x168, 0x166, 0x3, 0x2, 0x2, 0x2, 0x169, 0x16b, 
    0x5, 0xe2, 0x72, 0x2, 0x16a, 0x16c, 0x7, 0x7c, 0x2, 0x2, 0x16b, 0x16a, 
    0x3, 0x2, 0x2, 0x2, 0x16b, 0x16c, 0x3, 0x2, 0x2, 0x2, 0x16c, 0x16d, 
    0x3, 0x2, 0x2, 0x2, 0x16d, 0x16f, 0x7, 0x9, 0x2, 0x2, 0x16e, 0x170, 
    0x7, 0x7c, 0x2, 0x2, 0x16f, 0x16e, 0x3, 0x2, 0x2, 0x2, 0x16f, 0x170, 
    0x3, 0x2, 0x2, 0x2, 0x170, 0x171, 0x3, 0x2, 0x2, 0x2, 0x171, 0x172, 
    0x5, 0xba, 0x5e, 0x2, 0x172, 0xd, 0x3, 0x2, 0x2, 0x2, 0x173, 0x178, 
    0x5, 0x10, 0x9, 0x2, 0x174, 0x178, 0x5, 0x12, 0xa, 0x2, 0x175, 0x178, 
    0x5, 0x14, 0xb, 0x2, 0x176, 0x178, 0x5, 0x16, 0xc, 0x2, 0x177, 0x173, 
    0x3, 0x2, 0x2, 0x2, 0x177, 0x174, 0x3, 0x2, 0x2, 0x2, 0x177, 0x175, 
    0x3, 0x2, 0x2, 0x2, 0x177, 0x176, 0x3, 0x2, 0x2, 0x2, 0x178, 0xf, 0x3, 
    0x2, 0x2, 0x2, 0x179, 0x17a, 0x7, 0x48, 0x2, 0x2, 0x17a, 0x17b, 0x7, 
    0x7c, 0x2, 0x2, 0x17b, 0x17c, 0x7, 0x36, 0x2, 0x2, 0x17c, 0x17d, 0x7, 
    0x7c, 0x2, 0x2, 0x17d, 0x17e, 0x7, 0x37, 0x2, 0x2, 0x17e, 0x17f, 0x7, 
    0x7c, 0x2, 0x2, 0x17f, 0x181, 0x5, 0xe0, 0x71, 0x2, 0x180, 0x182, 0x7, 
    0x7c, 0x2, 0x2, 0x181, 0x180, 0x3, 0x2, 0x2, 0x2, 0x181, 0x182, 0x3, 
    0x2, 0x2, 0x2, 0x182, 0x183, 0x3, 0x2, 0x2, 0x2, 0x183, 0x185, 0x7, 
    0x4, 0x2, 0x2, 0x184, 0x186, 0x7, 0x7c, 0x2, 0x2, 0x185, 0x184, 0x3, 
    0x2, 0x2, 0x2, 0x185, 0x186, 0x3, 0x2, 0x2, 0x2, 0x186, 0x187, 0x3, 
    0x2, 0x2, 0x2, 0x187, 0x189, 0x5, 0x22, 0x12, 0x2, 0x188, 0x18a, 0x7, 
    0x7c, 0x2, 0x2, 0x189, 0x188, 0x3, 0x2, 0x2, 0x2, 0x189, 0x18a, 0x3, 
    0x2, 0x2, 0x2, 0x18a, 0x18b, 0x3, 0x2, 0x2, 0x2, 0x18b, 0x18d, 0x7, 
    0x6, 0x2, 0x2, 0x18c, 0x18e, 0x7, 0x7c, 0x2, 0x2, 0x18d, 0x18c, 0x3, 
    0x2, 0x2, 0x2, 0x18d, 0x18e, 0x3, 0x2, 0x2, 0x2, 0x18e, 0x18f, 0x3, 
    0x2, 0x2, 0x2, 0x18f, 0x190, 0x5, 0x26, 0x14, 0x2, 0x190, 0x192, 0x3, 
    0x2, 0x2, 0x2, 0x191, 0x193, 0x7, 0x7c, 0x2, 0x2, 0x192, 0x191, 0x3, 
    0x2, 0x2, 0x2, 0x192, 0x193, 0x3, 0x2, 0x2, 0x2, 0x193, 0x194, 0x3, 
    0x2, 0x2, 0x2, 0x194, 0x195, 0x7, 0x5, 0x2, 0x2, 0x195, 0x11, 0x3, 0x2, 
    0x2, 0x2, 0x196, 0x197, 0x7, 0x48, 0x2, 0x2, 0x197, 0x198, 0x7, 0x7c, 
    0x2, 0x2, 0x198, 0x199, 0x7, 0x3f, 0x2, 0x2, 0x199, 0x19a, 0x7, 0x7c, 
    0x2, 0x2, 0x19a, 0x19b, 0x7, 0x37, 0x2, 0x2, 0x19b, 0x19c, 0x7, 0x7c, 
    0x2, 0x2, 0x19c, 0x19e, 0x5, 0xe0, 0x71, 0x2, 0x19d, 0x19f, 0x7, 0x7c, 
    0x2, 0x2, 0x19e, 0x19d, 0x3, 0x2, 0x2, 0x2, 0x19e, 0x19f, 0x3, 0x2, 
    0x2, 0x2, 0x19f, 0x1a0, 0x3, 0x2, 0x2, 0x2, 0x1a0, 0x1a2, 0x7, 0x4, 
    0x2, 0x2, 0x1a1, 0x1a3, 0x7, 0x7c, 0x2, 0x2, 0x1a2, 0x1a1, 0x3, 0x2, 
    0x2, 0x2, 0x1a2, 0x1a3, 0x3, 0x2, 0x2, 0x2, 0x1a3, 0x1a4, 0x3, 0x2, 
    0x2, 0x2, 0x1a4, 0x1a5, 0x7, 0x33, 0x2, 0x2, 0x1a5, 0x1a6, 0x7, 0x7c, 
    0x2, 0x2, 0x1a6, 0x1a7, 0x5, 0xe0, 0x71, 0x2, 0x1a7, 0x1a8, 0x7, 0x7c, 
    0x2, 0x2, 0x1a8, 0x1a9, 0x7, 0x40, 0x2, 0x2, 0x1a9, 0x1aa, 0x7, 0x7c, 
    0x2, 0x2, 0x1aa, 0x1ac, 0x5, 0xe0, 0x71, 0x2, 0x1ab, 0x1ad, 0x7, 0x7c, 
    0x2, 0x2, 0x1ac, 0x1ab, 0x3, 0x2, 0x2, 0x2, 0x1ac, 0x1ad, 0x3, 0x2, 
    0x2, 0x2, 0x1ad, 0x1b6, 0x3, 0x2, 0x2, 0x2, 0x1ae, 0x1b0, 0x7, 0x6, 
    0x2, 0x2, 0x1af, 0x1b1, 0x7, 0x7c, 0x2, 0x2, 0x1b0, 0x1af, 0x3, 0x2, 
    0x2, 0x2, 0x1b0, 0x1b1, 0x3, 0x2, 0x2, 0x2, 0x1b1, 0x1b2, 0x3, 0x2, 
    0x2, 0x2, 0x1b2, 0x1b4, 0x5, 0x22, 0x12, 0x2, 0x1b3, 0x1b5, 0x7, 0x7c, 
    0x2, 0x2, 0x1b4, 0x1b3, 0x3, 0x2, 0x2, 0x2, 0x1b4, 0x1b5, 0x3, 0x2, 
    0x2, 0x2, 0x1b5, 0x1b7, 0x3, 0x2, 0x2, 0x2, 0x1b6, 0x1ae, 0x3, 0x2, 
    0x2, 0x2, 0x1b6, 0x1b7, 0x3, 0x2, 0x2, 0x2, 0x1b7, 0x1c0, 0x3, 0x2, 
    0x2, 0x2, 0x1b8, 0x1ba, 0x7, 0x6, 0x2, 0x2, 0x1b9, 0x1bb, 0x7, 0x7c, 
    0x2, 0x2, 0x1ba, 0x1b9, 0x3, 0x2, 0x2, 0x2, 0x1ba, 0x1bb, 0x3, 0x2, 
    0x2, 0x2, 0x1bb, 0x1bc, 0x3, 0x2, 0x2, 0x2, 0x1bc, 0x1be, 0x5, 0xe2, 
    0x72, 0x2, 0x1bd, 0x1bf, 0x7, 0x7c, 0x2, 0x2, 0x1be, 0x1bd, 0x3, 0x2, 
    0x2, 0x2, 0x1be, 0x1bf, 0x3, 0x2, 0x2, 0x2, 0x1bf, 0x1c1, 0x3, 0x2, 
    0x2, 0x2, 0x1c0, 0x1b8, 0x3, 0x2, 0x2, 0x2, 0x1c0, 0x1c1, 0x3, 0x2, 
    0x2, 0x2, 0x1c1, 0x1c2, 0x3, 0x2, 0x2, 0x2, 0x1c2, 0x1c3, 0x7, 0x5, 
    0x2, 0x2, 0x1c3, 0x13, 0x3, 0x2, 0x2, 0x2, 0x1c4, 0x1c5, 0x7, 0x38, 
    0x2, 0x2, 0x1c5, 0x1c6, 0x7, 0x7c, 0x2, 0x2, 0x1c6, 0x1c7, 0x7, 0x37, 
    0x2, 0x2, 0x1c7, 0x1c8, 0x7, 0x7c, 0x2, 0x2, 0x1c8, 0x1c9, 0x5, 0xe0, 
    0x71, 0x2, 0x1c9, 0x15, 0x3, 0x2, 0x2, 0x2, 0x1ca, 0x1cb, 0x7, 0x39, 
    0x2, 0x2, 0x1cb, 0x1cc, 0x7, 0x7c, 0x2, 0x2, 0x1cc, 0x1cd, 0x7, 0x37, 
    0x2, 0x2, 0x1cd, 0x1ce, 0x7, 0x7c, 0x2, 0x2, 0x1ce, 0x1cf, 0x5, 0xe0, 
    0x71, 0x2, 0x1cf, 0x1d0, 0x7, 0x7c, 0x2, 0x2, 0x1d0, 0x1d1, 0x5, 0x18, 
    0xd, 0x2, 0x1d1, 0x17, 0x3, 0x2, 0x2, 0x2, 0x1d2, 0x1d7, 0x5, 0x1a, 
    0xe, 0x2, 0x1d3, 0x1d7, 0x5, 0x1c, 0xf, 0x2, 0x1d4, 0x1d7, 0x5, 0x1e, 
    0x10, 0x2, 0x1d5, 0x1d7, 0x5, 0x20, 0x11, 0x2, 0x1d6, 0x1d2, 0x3, 0x2, 
    0x2, 0x2, 0x1d6, 0x1d3, 0x3, 0x2, 0x2, 0x2, 0x1d6, 0x1d4, 0x3, 0x2, 
    0x2, 0x2, 0x1d6, 0x1d5, 0x3, 0x2, 0x2, 0x2, 0x1d7, 0x19, 0x3, 0x2, 0x2, 
    0x2, 0x1d8, 0x1d9, 0x7, 0x3c, 0x2, 0x2, 0x1d9, 0x1da, 0x7, 0x7c, 0x2, 
    0x2, 0x1da, 0x1db, 0x5, 0xda, 0x6e, 0x2, 0x1db, 0x1dc, 0x7, 0x7c, 0x2, 
    0x2, 0x1dc, 0x1e1, 0x5, 0x28, 0x15, 0x2, 0x1dd, 0x1de, 0x7, 0x7c, 0x2, 
    0x2, 0x1de, 0x1df, 0x7, 0x3a, 0x2, 0x2, 0x1df, 0x1e0, 0x7, 0x7c, 0x2, 
    0x2, 0x1e0, 0x1e2, 0x5, 0x86, 0x44, 0x2, 0x1e1, 0x1dd, 0x3, 0x2, 0x2, 
    0x2, 0x1e1, 0x1e2, 0x3, 0x2, 0x2, 0x2, 0x1e2, 0x1b, 0x3, 0x2, 0x2, 0x2, 
    0x1e3, 0x1e4, 0x7, 0x38, 0x2, 0x2, 0x1e4, 0x1e5, 0x7, 0x7c, 0x2, 0x2, 
    0x1e5, 0x1e6, 0x5, 0xda, 0x6e, 0x2, 0x1e6, 0x1d, 0x3, 0x2, 0x2, 0x2, 
    0x1e7, 0x1e8, 0x7, 0x3b, 0x2, 0x2, 0x1e8, 0x1e9, 0x7, 0x7c, 0x2, 0x2, 
    0x1e9, 0x1ea, 0x7, 0x40, 0x2, 0x2, 0x1ea, 0x1eb, 0x7, 0x7c, 0x2, 0x2, 
    0x1eb, 0x1ec, 0x5, 0xe0, 0x71, 0x2, 0x1ec, 0x1f, 0x3, 0x2, 0x2, 0x2, 
    0x1ed, 0x1ee, 0x7, 0x3b, 0x2, 0x2, 0x1ee, 0x1ef, 0x7, 0x7c, 0x2, 0x2, 
    0x1ef, 0x1f0, 0x5, 0xda, 0x6e, 0x2, 0x1f0, 0x1f1, 0x7, 0x7c, 0x2, 0x2, 
    0x1f1, 0x1f2, 0x7, 0x40, 0x2, 0x2, 0x1f2, 0x1f3, 0x7, 0x7c, 0x2, 0x2, 
    0x1f3, 0x1f4, 0x5, 0xda, 0x6e, 0x2, 0x1f4, 0x21, 0x3, 0x2, 0x2, 0x2, 
    0x1f5, 0x200, 0x5, 0x24, 0x13, 0x2, 0x1f6, 0x1f8, 0x7, 0x7c, 0x2, 0x2, 
    0x1f7, 0x1f6, 0x3, 0x2, 0x2, 0x2, 0x1f7, 0x1f8, 0x3, 0x2, 0x2, 0x2, 
    0x1f8, 0x1f9, 0x3, 0x2, 0x2, 0x2, 0x1f9, 0x1fb, 0x7, 0x6, 0x2, 0x2, 
    0x1fa, 0x1fc, 0x7, 0x7c, 0x2, 0x2, 0x1fb, 0x1fa, 0x3, 0x2, 0x2, 0x2, 
    0x1fb, 0x1fc, 0x3, 0x2, 0x2, 0x2, 0x1fc, 0x1fd, 0x3, 0x2, 0x2, 0x2, 
    0x1fd, 0x1ff, 0x5, 0x24, 0x13, 0x2, 0x1fe, 0x1f7, 0x3, 0x2, 0x2, 0x2, 
    0x1ff, 0x202, 0x3, 0x2, 0x2, 0x2, 0x200, 0x1fe, 0x3, 0x2, 0x2, 0x2, 
    0x200, 0x201, 0x3, 0x2, 0x2, 0x2, 0x201, 0x23, 0x3, 0x2, 0x2, 0x2, 0x202, 
    0x200, 0x3, 0x2, 0x2, 0x2, 0x203, 0x204, 0x5, 0xda, 0x6e, 0x2, 0x204, 
    0x205, 0x7, 0x7c, 0x2, 0x2, 0x205, 0x206, 0x5, 0x28, 0x15, 0x2, 0x206, 
    0x25, 0x3, 0x2, 0x2, 0x2, 0x207, 0x208, 0x7, 0x3d, 0x2, 0x2, 0x208, 
    0x209, 0x7, 0x7c, 0x2, 0x2, 0x209, 0x20b, 0x7, 0x3e, 0x2, 0x2, 0x20a, 
    0x20c, 0x7, 0x7c, 0x2, 0x2, 0x20b, 0x20a, 0x3, 0x2, 0x2, 0x2, 0x20b, 
    0x20c, 0x3, 0x2, 0x2, 0x2, 0x20c, 0x20d, 0x3, 0x2, 0x2, 0x2, 0x20d, 
    0x20f, 0x7, 0x4, 0x2, 0x2, 0x20e, 0x210, 0x7, 0x7c, 0x2, 0x2, 0x20f, 
    0x20e, 0x3, 0x2, 0x2, 0x2, 0x20f, 0x210, 0x3, 0x2, 0x2, 0x2, 0x210, 
    0x211, 0x3, 0x2, 0x2, 0x2, 0x211, 0x213, 0x5, 0xda, 0x6e, 0x2, 0x212, 
    0x214, 0x7, 0x7c, 0x2, 0x2, 0x213, 0x212, 0x3, 0x2, 0x2, 0x2, 0x213, 
    0x214, 0x3, 0x2, 0x2, 0x2, 0x214, 0x215, 0x3, 0x2, 0x2, 0x2, 0x215, 
    0x216, 0x7, 0x5, 0x2, 0x2, 0x216, 0x27, 0x3, 0x2, 0x2, 0x2, 0x217, 0x22a, 
    0x5, 0xe2, 0x72, 0x2, 0x218, 0x219, 0x5, 0xe2, 0x72, 0x2, 0x219, 0x21a, 
    0x5, 0x2a, 0x16, 0x2, 0x21a, 0x22a, 0x3, 0x2, 0x2, 0x2, 0x21b, 0x21d, 
    0x5, 0xe2, 0x72, 0x2, 0x21c, 0x21e, 0x7, 0x7c, 0x2, 0x2, 0x21d, 0x21c, 
    0x3, 0x2, 0x2, 0x2, 0x21d, 0x21e, 0x3, 0x2, 0x2, 0x2, 0x21e, 0x21f, 
    0x3, 0x2, 0x2, 0x2, 0x21f, 0x221, 0x7, 0x4, 0x2, 0x2, 0x220, 0x222, 
    0x7, 0x7c, 0x2, 0x2, 0x221, 0x220, 0x3, 0x2, 0x2, 0x2, 0x221, 0x222, 
    0x3, 0x2, 0x2, 0x2, 0x222, 0x223, 0x3, 0x2, 0x2, 0x2, 0x223, 0x225, 
    0x5, 0x22, 0x12, 0x2, 0x224, 0x226, 0x7, 0x7c, 0x2, 0x2, 0x225, 0x224, 
    0x3, 0x2, 0x2, 0x2, 0x225, 0x226, 0x3, 0x2, 0x2, 0x2, 0x226, 0x227, 
    0x3, 0x2, 0x2, 0x2, 0x227, 0x228, 0x7, 0x5, 0x2, 0x2, 0x228, 0x22a, 
    0x3, 0x2, 0x2, 0x2, 0x229, 0x217, 0x3, 0x2, 0x2, 0x2, 0x229, 0x218, 
    0x3, 0x2, 0x2, 0x2, 0x229, 0x21b, 0x3, 0x2, 0x2, 0x2, 0x22a, 0x29, 0x3, 
    0x2, 0x2, 0x2, 0x22b, 0x22f, 0x5, 0x2c, 0x17, 0x2, 0x22c, 0x22e, 0x5, 
    0x2c, 0x17, 0x2, 0x22d, 0x22c, 0x3, 0x2, 0x2, 0x2, 0x22e, 0x231, 0x3, 
    0x2, 0x2, 0x2, 0x22f, 0x22d, 0x3, 0x2, 0x2, 0x2, 0x22f, 0x230, 0x3, 
    0x2, 0x2, 0x2, 0x230, 0x2b, 0x3, 0x2, 0x2, 0x2, 0x231, 0x22f, 0x3, 0x2, 
    0x2, 0x2, 0x232, 0x234, 0x7, 0x7, 0x2, 0x2, 0x233, 0x235, 0x5, 0xdc, 
    0x6f, 0x2, 0x234, 0x233, 0x3, 0x2, 0x2, 0x2, 0x234, 0x235, 0x3, 0x2, 
    0x2, 0x2, 0x235, 0x236, 0x3, 0x2, 0x2, 0x2, 0x236, 0x237, 0x7, 0x8, 
    0x2, 0x2, 0x237, 0x2d, 0x3, 0x2, 0x2, 0x2, 0x238, 0x23b, 0x5, 0x30, 
    0x19, 0x2, 0x239, 0x23b, 0x5, 0x32, 0x1a, 0x2, 0x23a, 0x238, 0x3, 0x2, 
    0x2, 0x2, 0x23a, 0x239, 0x3, 0x2, 0x2, 0x2, 0x23b, 0x2f, 0x3, 0x2, 0x2, 
    0x2, 0x23c, 0x23d, 0x7, 0x41, 0x2, 0x2, 0x23d, 0x31, 0x3, 0x2, 0x2, 
    0x2, 0x23e, 0x23f, 0x7, 0x42, 0x2, 0x2, 0x23f, 0x33, 0x3, 0x2, 0x2, 
    0x2, 0x240, 0x241, 0x5, 0x36, 0x1c, 0x2, 0x241, 0x35, 0x3, 0x2, 0x2, 
    0x2, 0x242, 0x243, 0x5, 0x38, 0x1d, 0x2, 0x243, 0x37, 0x3, 0x2, 0x2, 
    0x2, 0x244, 0x24b, 0x5, 0x3c, 0x1f, 0x2, 0x245, 0x247, 0x7, 0x7c, 0x2, 
    0x2, 0x246, 0x245, 0x3, 0x2, 0x2, 0x2, 0x246, 0x247, 0x3, 0x2, 0x2, 
    0x2, 0x247, 0x248, 0x3, 0x2, 0x2, 0x2, 0x248, 0x24a, 0x5, 0x3a, 0x1e, 
    0x2, 0x249, 0x246, 0x3, 0x2, 0x2, 0x2, 0x24a, 0x24d, 0x3, 0x2, 0x2, 
    0x2, 0x24b, 0x249, 0x3, 0x2, 0x2, 0x2, 0x24b, 0x24c, 0x3, 0x2, 0x2, 
    0x2, 0x24c, 0x25a, 0x3, 0x2, 0x2, 0x2, 0x24d, 0x24b, 0x3, 0x2, 0x2, 
    0x2, 0x24e, 0x250, 0x5, 0x56, 0x2c, 0x2, 0x24f, 0x251, 0x7, 0x7c, 0x2, 
    0x2, 0x250, 0x24f, 0x3, 0x2, 0x2, 0x2, 0x250, 0x251, 0x3, 0x2, 0x2, 
    0x2, 0x251, 0x253, 0x3, 0x2, 0x2, 0x2, 0x252, 0x24e, 0x3, 0x2, 0x2, 
    0x2, 0x253, 0x254, 0x3, 0x2, 0x2, 0x2, 0x254, 0x252, 0x3, 0x2, 0x2, 
    0x2, 0x254, 0x255, 0x3, 0x2, 0x2, 0x2, 0x255, 0x256, 0x3, 0x2, 0x2, 
    0x2, 0x256, 0x257, 0x5, 0x3c, 0x1f, 0x2, 0x257, 0x258, 0x8, 0x1d, 0x1, 
    0x2, 0x258, 0x25a, 0x3, 0x2, 0x2, 0x2, 0x259, 0x244, 0x3, 0x2, 0x2, 
    0x2, 0x259, 0x252, 0x3, 0x2, 0x2, 0x2, 0x25a, 0x39, 0x3, 0x2, 0x2, 0x2, 
    0x25b, 0x25c, 0x7, 0x43, 0x2, 0x2, 0x25c, 0x25d, 0x7, 0x7c, 0x2, 0x2, 
    0x25d, 0x25f, 0x7, 0x44, 0x2, 0x2, 0x25e, 0x260, 0x7, 0x7c, 0x2, 0x2, 
    0x25f, 0x25e, 0x3, 0x2, 0x2, 0x2, 0x25f, 0x260, 0x3, 0x2, 0x2, 0x2, 
    0x260, 0x261, 0x3, 0x2, 0x2, 0x2, 0x261, 0x268, 0x5, 0x3c, 0x1f, 0x2, 
    0x262, 0x264, 0x7, 0x43, 0x2, 0x2, 0x263, 0x265, 0x7, 0x7c, 0x2, 0x2, 
    0x264, 0x263, 0x3, 0x2, 0x2, 0x2, 0x264, 0x265, 0x3, 0x2, 0x2, 0x2, 
    0x265, 0x266, 0x3, 0x2, 0x2, 0x2, 0x266, 0x268, 0x5, 0x3c, 0x1f, 0x2, 
    0x267, 0x25b, 0x3, 0x2, 0x2, 0x2, 0x267, 0x262, 0x3, 0x2, 0x2, 0x2, 
    0x268, 0x3b, 0x3, 0x2, 0x2, 0x2, 0x269, 0x26c, 0x5, 0x3e, 0x20, 0x2, 
    0x26a, 0x26c, 0x5, 0x40, 0x21, 0x2, 0x26b, 0x269, 0x3, 0x2, 0x2, 0x2, 
    0x26b, 0x26a, 0x3, 0x2, 0x2, 0x2, 0x26c, 0x3d, 0x3, 0x2, 0x2, 0x2, 0x26d, 
    0x26f, 0x5, 0x46, 0x24, 0x2, 0x26e, 0x270, 0x7, 0x7c, 0x2, 0x2, 0x26f, 
    0x26e, 0x3, 0x2, 0x2, 0x2, 0x26f, 0x270, 0x3, 0x2, 0x2, 0x2, 0x270, 
    0x272, 0x3, 0x2, 0x2, 0x2, 0x271, 0x26d, 0x3, 0x2, 0x2, 0x2, 0x272, 
    0x275, 0x3, 0x2, 0x2, 0x2, 0x273, 0x271, 0x3, 0x2, 0x2, 0x2, 0x273, 
    0x274, 0x3, 0x2, 0x2, 0x2, 0x274, 0x276, 0x3, 0x2, 0x2, 0x2, 0x275, 
    0x273, 0x3, 0x2, 0x2, 0x2, 0x276, 0x29b, 0x5, 0x56, 0x2c, 0x2, 0x277, 
    0x279, 0x5, 0x46, 0x24, 0x2, 0x278, 0x27a, 0x7, 0x7c, 0x2, 0x2, 0x279, 
    0x278, 0x3, 0x2, 0x2, 0x2, 0x279, 0x27a, 0x3, 0x2, 0x2, 0x2, 0x27a, 
    0x27c, 0x3, 0x2, 0x2, 0x2, 0x27b, 0x277, 0x3, 0x2, 0x2, 0x2, 0x27c, 
    0x27f, 0x3, 0x2, 0x2, 0x2, 0x27d, 0x27b, 0x3, 0x2, 0x2, 0x2, 0x27d, 
    0x27e, 0x3, 0x2, 0x2, 0x2, 0x27e, 0x280, 0x3, 0x2, 0x2, 0x2, 0x27f, 
    0x27d, 0x3, 0x2, 0x2, 0x2, 0x280, 0x287, 0x5, 0x44, 0x23, 0x2, 0x281, 
    0x283, 0x7, 0x7c, 0x2, 0x2, 0x282, 0x281, 0x3, 0x2, 0x2, 0x2, 0x282, 
    0x283, 0x3, 0x2, 0x2, 0x2, 0x283, 0x284, 0x3, 0x2, 0x2, 0x2, 0x284, 
    0x286, 0x5, 0x44, 0x23, 0x2, 0x285, 0x282, 0x3, 0x2, 0x2, 0x2, 0x286, 
    0x289, 0x3, 0x2, 0x2, 0x2, 0x287, 0x285, 0x3, 0x2, 0x2, 0x2, 0x287, 
    0x288, 0x3, 0x2, 0x2, 0x2, 0x288, 0x28e, 0x3, 0x2, 0x2, 0x2, 0x289, 
    0x287, 0x3, 0x2, 0x2, 0x2, 0x28a, 0x28c, 0x7, 0x7c, 0x2, 0x2, 0x28b, 
    0x28a, 0x3, 0x2, 0x2, 0x2, 0x28b, 0x28c, 0x3, 0x2, 0x2, 0x2, 0x28c, 
    0x28d, 0x3, 0x2, 0x2, 0x2, 0x28d, 0x28f, 0x5, 0x56, 0x2c, 0x2, 0x28e, 
    0x28b, 0x3, 0x2, 0x2, 0x2, 0x28e, 0x28f, 0x3, 0x2, 0x2, 0x2, 0x28f, 
    0x29b, 0x3, 0x2, 0x2, 0x2, 0x290, 0x292, 0x5, 0x46, 0x24, 0x2, 0x291, 
    0x293, 0x7, 0x7c, 0x2, 0x2, 0x292, 0x291, 0x3, 0x2, 0x2, 0x2, 0x292, 
    0x293, 0x3, 0x2, 0x2, 0x2, 0x293, 0x295, 0x3, 0x2, 0x2, 0x2, 0x294, 
    0x290, 0x3, 0x2, 0x2, 0x2, 0x295, 0x298, 0x3, 0x2, 0x2, 0x2, 0x296, 
    0x294, 0x3, 0x2, 0x2, 0x2, 0x296, 0x297, 0x3, 0x2, 0x2, 0x2, 0x297, 
    0x299, 0x3, 0x2, 0x2, 0x2, 0x298, 0x296, 0x3, 0x2, 0x2, 0x2, 0x299, 
    0x29b, 0x8, 0x20, 0x1, 0x2, 0x29a, 0x273, 0x3, 0x2, 0x2, 0x2, 0x29a, 
    0x27d, 0x3, 0x2, 0x2, 0x2, 0x29a, 0x296, 0x3, 0x2, 0x2, 0x2, 0x29b, 
    0x3f, 0x3, 0x2, 0x2, 0x2, 0x29c, 0x29e, 0x5, 0x42, 0x22, 0x2, 0x29d, 
    0x29f, 0x7, 0x7c, 0x2, 0x2, 0x29e, 0x29d, 0x3, 0x2, 0x2, 0x2, 0x29e, 
    0x29f, 0x3, 0x2, 0x2, 0x2, 0x29f, 0x2a1, 0x3, 0x2, 0x2, 0x2, 0x2a0, 
    0x29c, 0x3, 0x2, 0x2, 0x2, 0x2a1, 0x2a2, 0x3, 0x2, 0x2, 0x2, 0x2a2, 
    0x2a0, 0x3, 0x2, 0x2, 0x2, 0x2a2, 0x2a3, 0x3, 0x2, 0x2, 0x2, 0x2a3, 
    0x2a4, 0x3, 0x2, 0x2, 0x2, 0x2a4, 0x2a5, 0x5, 0x3e, 0x20, 0x2, 0x2a5, 
    0x41, 0x3, 0x2, 0x2, 0x2, 0x2a6, 0x2a8, 0x5, 0x46, 0x24, 0x2, 0x2a7, 
    0x2a9, 0x7, 0x7c, 0x2, 0x2, 0x2a8, 0x2a7, 0x3, 0x2, 0x2, 0x2, 0x2a8, 
    0x2a9, 0x3, 0x2, 0x2, 0x2, 0x2a9, 0x2ab, 0x3, 0x2, 0x2, 0x2, 0x2aa, 
    0x2a6, 0x3, 0x2, 0x2, 0x2, 0x2ab, 0x2ae, 0x3, 0x2, 0x2, 0x2, 0x2ac, 
    0x2aa, 0x3, 0x2, 0x2, 0x2, 0x2ac, 0x2ad, 0x3, 0x2, 0x2, 0x2, 0x2ad, 
    0x2b5, 0x3, 0x2, 0x2, 0x2, 0x2ae, 0x2ac, 0x3, 0x2, 0x2, 0x2, 0x2af, 
    0x2b1, 0x5, 0x44, 0x23, 0x2, 0x2b0, 0x2b2, 0x7, 0x7c, 0x2, 0x2, 0x2b1, 
    0x2b0, 0x3, 0x2, 0x2, 0x2, 0x2b1, 0x2b2, 0x3, 0x2, 0x2, 0x2, 0x2b2, 
    0x2b4, 0x3, 0x2, 0x2, 0x2, 0x2b3, 0x2af, 0x3, 0x2, 0x2, 0x2, 0x2b4, 
    0x2b7, 0x3, 0x2, 0x2, 0x2, 0x2b5, 0x2b3, 0x3, 0x2, 0x2, 0x2, 0x2b5, 
    0x2b6, 0x3, 0x2, 0x2, 0x2, 0x2b6, 0x2b8, 0x3, 0x2, 0x2, 0x2, 0x2b7, 
    0x2b5, 0x3, 0x2, 0x2, 0x2, 0x2b8, 0x2b9, 0x5, 0x54, 0x2b, 0x2, 0x2b9, 
    0x43, 0x3, 0x2, 0x2, 0x2, 0x2ba, 0x2be, 0x5, 0x4c, 0x27, 0x2, 0x2bb, 
    0x2be, 0x5, 0x4e, 0x28, 0x2, 0x2bc, 0x2be, 0x5, 0x52, 0x2a, 0x2, 0x2bd, 
    0x2ba, 0x3, 0x2, 0x2, 0x2, 0x2bd, 0x2bb, 0x3, 0x2, 0x2, 0x2, 0x2bd, 
    0x2bc, 0x3, 0x2, 0x2, 0x2, 0x2be, 0x45, 0x3, 0x2, 0x2, 0x2, 0x2bf, 0x2c2, 
    0x5, 0x48, 0x25, 0x2, 0x2c0, 0x2c2, 0x5, 0x4a, 0x26, 0x2, 0x2c1, 0x2bf, 
    0x3, 0x2, 0x2, 0x2, 0x2c1, 0x2c0, 0x3, 0x2, 0x2, 0x2, 0x2c2, 0x47, 0x3, 
    0x2, 0x2, 0x2, 0x2c3, 0x2c4, 0x7, 0x45, 0x2, 0x2, 0x2c4, 0x2c6, 0x7, 
    0x7c, 0x2, 0x2, 0x2c5, 0x2c3, 0x3, 0x2, 0x2, 0x2, 0x2c5, 0x2c6, 0x3, 
    0x2, 0x2, 0x2, 0x2c6, 0x2c7, 0x3, 0x2, 0x2, 0x2, 0x2c7, 0x2c9, 0x7, 
    0x46, 0x2, 0x2, 0x2c8, 0x2ca, 0x7, 0x7c, 0x2, 0x2, 0x2c9, 0x2c8, 0x3, 
    0x2, 0x2, 0x2, 0x2c9, 0x2ca, 0x3, 0x2, 0x2, 0x2, 0x2ca, 0x2cb, 0x3, 
    0x2, 0x2, 0x2, 0x2cb, 0x2d0, 0x5, 0x68, 0x35, 0x2, 0x2cc, 0x2ce, 0x7, 
    0x7c, 0x2, 0x2, 0x2cd, 0x2cc, 0x3, 0x2, 0x2, 0x2, 0x2cd, 0x2ce, 0x3, 
    0x2, 0x2, 0x2, 0x2ce, 0x2cf, 0x3, 0x2, 0x2, 0x2, 0x2cf, 0x2d1, 0x5, 
    0x66, 0x34, 0x2, 0x2d0, 0x2cd, 0x3, 0x2, 0x2, 0x2, 0x2d0, 0x2d1, 0x3, 
    0x2, 0x2, 0x2, 0x2d1, 0x49, 0x3, 0x2, 0x2, 0x2, 0x2d2, 0x2d4, 0x7, 0x47, 
    0x2, 0x2, 0x2d3, 0x2d5, 0x7, 0x7c, 0x2, 0x2, 0x2d4, 0x2d3, 0x3, 0x2, 
    0x2, 0x2, 0x2d4, 0x2d5, 0x3, 0x2, 0x2, 0x2, 0x2d5, 0x2d6, 0x3, 0x2, 
    0x2, 0x2, 0x2d6, 0x2d7, 0x5, 0x86, 0x44, 0x2, 0x2d7, 0x2d8, 0x7, 0x7c, 
    0x2, 0x2, 0x2d8, 0x2d9, 0x7, 0x4f, 0x2, 0x2, 0x2d9, 0x2da, 0x7, 0x7c, 
    0x2, 0x2, 0x2da, 0x2db, 0x5, 0xd2, 0x6a, 0x2, 0x2db, 0x4b, 0x3, 0x2, 
    0x2, 0x2, 0x2dc, 0x2de, 0x7, 0x48, 0x2, 0x2, 0x2dd, 0x2df, 0x7, 0x7c, 
    0x2, 0x2, 0x2de, 0x2dd, 0x3, 0x2, 0x2, 0x2, 0x2de, 0x2df, 0x3, 0x2, 
    0x2, 0x2, 0x2df, 0x2e0, 0x3, 0x2, 0x2, 0x2, 0x2e0, 0x2e1, 0x5, 0x68, 
    0x35, 0x2, 0x2e1, 0x4d, 0x3, 0x2, 0x2, 0x2, 0x2e2, 0x2e4, 0x7, 0x49, 
    0x2, 0x2, 0x2e3, 0x2e5, 0x7, 0x7c, 0x2, 0x2, 0x2e4, 0x2e3, 0x3, 0x2, 
    0x2, 0x2, 0x2e4, 0x2e5, 0x3, 0x2, 0x2, 0x2, 0x2e5, 0x2e6, 0x3, 0x2, 
    0x2, 0x2, 0x2e6, 0x2f1, 0x5, 0x50, 0x29, 0x2, 0x2e7, 0x2e9, 0x7, 0x7c, 
    0x2, 0x2, 0x2e8, 0x2e7, 0x3, 0x2, 0x2, 0x2, 0x2e8, 0x2e9, 0x3, 0x2, 
    0x2, 0x2, 0x2e9, 0x2ea, 0x3, 0x2, 0x2, 0x2, 0x2ea, 0x2ec, 0x7, 0x6, 
    0x2, 0x2, 0x2eb, 0x2ed, 0x7, 0x7c, 0x2, 0x2, 0x2ec, 0x2eb, 0x3, 0x2, 
    0x2, 0x2, 0x2ec, 0x2ed, 0x3, 0x2, 0x2, 0x2, 0x2ed, 0x2ee, 0x3, 0x2, 
    0x2, 0x2, 0x2ee, 0x2f0, 0x5, 0x50, 0x29, 0x2, 0x2ef, 0x2e8, 0x3, 0x2, 
    0x2, 0x2, 0x2f0, 0x2f3, 0x3, 0x2, 0x2, 0x2, 0x2f1, 0x2ef, 0x3, 0x2, 
    0x2, 0x2, 0x2f1, 0x2f2, 0x3, 0x2, 0x2, 0x2, 0x2f2, 0x4f, 0x3, 0x2, 0x2, 
    0x2, 0x2f3, 0x2f1, 0x3, 0x2, 0x2, 0x2, 0x2f4, 0x2f6, 0x5, 0xd8, 0x6d, 
    0x2, 0x2f5, 0x2f7, 0x7, 0x7c, 0x2, 0x2, 0x2f6, 0x2f5, 0x3, 0x2, 0x2, 
    0x2, 0x2f6, 0x2f7, 0x3, 0x2, 0x2, 0x2, 0x2f7, 0x2f8, 0x3, 0x2, 0x2, 
    0x2, 0x2f8, 0x2fa, 0x7, 0x9, 0x2, 0x2, 0x2f9, 0x2fb, 0x7, 0x7c, 0x2, 
    0x2, 0x2fa, 0x2f9, 0x3, 0x2, 0x2, 0x2, 0x2fa, 0x2fb, 0x3, 0x2, 0x2, 
    0x2, 0x2fb, 0x2fc, 0x3, 0x2, 0x2, 0x2, 0x2fc, 0x2fd, 0x5, 0x86, 0x44, 
    0x2, 0x2fd, 0x51, 0x3, 0x2, 0x2, 0x2, 0x2fe, 0x300, 0x7, 0x4a, 0x2, 
    0x2, 0x2ff, 0x301, 0x7, 0x7c, 0x2, 0x2, 0x300, 0x2ff, 0x3, 0x2, 0x2, 
    0x2, 0x300, 0x301, 0x3, 0x2, 0x2, 0x2, 0x301, 0x302, 0x3, 0x2, 0x2, 
    0x2, 0x302, 0x30d, 0x5, 0x86, 0x44, 0x2, 0x303, 0x305, 0x7, 0x7c, 0x2, 
    0x2, 0x304, 0x303, 0x3, 0x2, 0x2, 0x2, 0x304, 0x305, 0x3, 0x2, 0x2, 
    0x2, 0x305, 0x306, 0x3, 0x2, 0x2, 0x2, 0x306, 0x308, 0x7, 0x6, 0x2, 
    0x2, 0x307, 0x309, 0x7, 0x7c, 0x2, 0x2, 0x308, 0x307, 0x3, 0x2, 0x2, 
    0x2, 0x308, 0x309, 0x3, 0x2, 0x2, 0x2, 0x309, 0x30a, 0x3, 0x2, 0x2, 
    0x2, 0x30a, 0x30c, 0x5, 0x86, 0x44, 0x2, 0x30b, 0x304, 0x3, 0x2, 0x2, 
    0x2, 0x30c, 0x30f, 0x3, 0x2, 0x2, 0x2, 0x30d, 0x30b, 0x3, 0x2, 0x2, 
    0x2, 0x30d, 0x30e, 0x3, 0x2, 0x2, 0x2, 0x30e, 0x53, 0x3, 0x2, 0x2, 0x2, 
    0x30f, 0x30d, 0x3, 0x2, 0x2, 0x2, 0x310, 0x311, 0x7, 0x4b, 0x2, 0x2, 
    0x311, 0x316, 0x5, 0x58, 0x2d, 0x2, 0x312, 0x314, 0x7, 0x7c, 0x2, 0x2, 
    0x313, 0x312, 0x3, 0x2, 0x2, 0x2, 0x313, 0x314, 0x3, 0x2, 0x2, 0x2, 
    0x314, 0x315, 0x3, 0x2, 0x2, 0x2, 0x315, 0x317, 0x5, 0x66, 0x34, 0x2, 
    0x316, 0x313, 0x3, 0x2, 0x2, 0x2, 0x316, 0x317, 0x3, 0x2, 0x2, 0x2, 
    0x317, 0x55, 0x3, 0x2, 0x2, 0x2, 0x318, 0x319, 0x7, 0x4c, 0x2, 0x2, 
    0x319, 0x31a, 0x5, 0x58, 0x2d, 0x2, 0x31a, 0x57, 0x3, 0x2, 0x2, 0x2, 
    0x31b, 0x31d, 0x7, 0x7c, 0x2, 0x2, 0x31c, 0x31b, 0x3, 0x2, 0x2, 0x2, 
    0x31c, 0x31d, 0x3, 0x2, 0x2, 0x2, 0x31d, 0x31e, 0x3, 0x2, 0x2, 0x2, 
    0x31e, 0x320, 0x7, 0x4d, 0x2, 0x2, 0x31f, 0x31c, 0x3, 0x2, 0x2, 0x2, 
    0x31f, 0x320, 0x3, 0x2, 0x2, 0x2, 0x320, 0x321, 0x3, 0x2, 0x2, 0x2, 
    0x321, 0x322, 0x7, 0x7c, 0x2, 0x2, 0x322, 0x325, 0x5, 0x5a, 0x2e, 0x2, 
    0x323, 0x324, 0x7, 0x7c, 0x2, 0x2, 0x324, 0x326, 0x5, 0x5e, 0x30, 0x2, 
    0x325, 0x323, 0x3, 0x2, 0x2, 0x2, 0x325, 0x326, 0x3, 0x2, 0x2, 0x2, 
    0x326, 0x329, 0x3, 0x2, 0x2, 0x2, 0x327, 0x328, 0x7, 0x7c, 0x2, 0x2, 
    0x328, 0x32a, 0x5, 0x60, 0x31, 0x2, 0x329, 0x327, 0x3, 0x2, 0x2, 0x2, 
    0x329, 0x32a, 0x3, 0x2, 0x2, 0x2, 0x32a, 0x32d, 0x3, 0x2, 0x2, 0x2, 
    0x32b, 0x32c, 0x7, 0x7c, 0x2, 0x2, 0x32c, 0x32e, 0x5, 0x62, 0x32, 0x2, 
    0x32d, 0x32b, 0x3, 0x2, 0x2, 0x2, 0x32d, 0x32e, 0x3, 0x2, 0x2, 0x2, 
    0x32e, 0x59, 0x3, 0x2, 0x2, 0x2, 0x32f, 0x33a, 0x7, 0x4e, 0x2, 0x2, 
    0x330, 0x332, 0x7, 0x7c, 0x2, 0x2, 0x331, 0x330, 0x3, 0x2, 0x2, 0x2, 
    0x331, 0x332, 0x3, 0x2, 0x2, 0x2, 0x332, 0x333, 0x3, 0x2, 0x2, 0x2, 
    0x333, 0x335, 0x7, 0x6, 0x2, 0x2, 0x334, 0x336, 0x7, 0x7c, 0x2, 0x2, 
    0x335, 0x334, 0x3, 0x2, 0x2, 0x2, 0x335, 0x336, 0x3, 0x2, 0x2, 0x2, 
    0x336, 0x337, 0x3, 0x2, 0x2, 0x2, 0x337, 0x339, 0x5, 0x5c, 0x2f, 0x2, 
    0x338, 0x331, 0x3, 0x2, 0x2, 0x2, 0x339, 0x33c, 0x3, 0x2, 0x2, 0x2, 
    0x33a, 0x338, 0x3, 0x2, 0x2, 0x2, 0x33a, 0x33b, 0x3, 0x2, 0x2, 0x2, 
    0x33b, 0x34c, 0x3, 0x2, 0x2, 0x2, 0x33c, 0x33a, 0x3, 0x2, 0x2, 0x2, 
    0x33d, 0x348, 0x5, 0x5c, 0x2f, 0x2, 0x33e, 0x340, 0x7, 0x7c, 0x2, 0x2, 
    0x33f, 0x33e, 0x3, 0x2, 0x2, 0x2, 0x33f, 0x340, 0x3, 0x2, 0x2, 0x2, 
    0x340, 0x341, 0x3, 0x2, 0x2, 0x2, 0x341, 0x343, 0x7, 0x6, 0x2, 0x2, 
    0x342, 0x344, 0x7, 0x7c, 0x2, 0x2, 0x343, 0x342, 0x3, 0x2, 0x2, 0x2, 
    0x343, 0x344, 0x3, 0x2, 0x2, 0x2, 0x344, 0x345, 0x3, 0x2, 0x2, 0x2, 
    0x345, 0x347, 0x5, 0x5c, 0x2f, 0x2, 0x346, 0x33f, 0x3, 0x2, 0x2, 0x2, 
    0x347, 0x34a, 0x3, 0x2, 0x2, 0x2, 0x348, 0x346, 0x3, 0x2, 0x2, 0x2, 
    0x348, 0x349, 0x3, 0x2, 0x2, 0x2, 0x349, 0x34c, 0x3, 0x2, 0x2, 0x2, 
    0x34a, 0x348, 0x3, 0x2, 0x2, 0x2, 0x34b, 0x32f, 0x3, 0x2, 0x2, 0x2, 
    0x34b, 0x33d, 0x3, 0x2, 0x2, 0x2, 0x34c, 0x5b, 0x3, 0x2, 0x2, 0x2, 0x34d, 
    0x34e, 0x5, 0x86, 0x44, 0x2, 0x34e, 0x34f, 0x7, 0x7c, 0x2, 0x2, 0x34f, 
    0x350, 0x7, 0x4f, 0x2, 0x2, 0x350, 0x351, 0x7, 0x7c, 0x2, 0x2, 0x351, 
    0x352, 0x5, 0xd2, 0x6a, 0x2, 0x352, 0x355, 0x3, 0x2, 0x2, 0x2, 0x353, 
    0x355, 0x5, 0x86, 0x44, 0x2, 0x354, 0x34d, 0x3, 0x2, 0x2, 0x2, 0x354, 
    0x353, 0x3, 0x2, 0x2, 0x2, 0x355, 0x5d, 0x3, 0x2, 0x2, 0x2, 0x356, 0x357, 
    0x7, 0x50, 0x2, 0x2, 0x357, 0x358, 0x7, 0x7c, 0x2, 0x2, 0x358, 0x359, 
    0x7, 0x51, 0x2, 0x2, 0x359, 0x35a, 0x7, 0x7c, 0x2, 0x2, 0x35a, 0x362, 
    0x5, 0x64, 0x33, 0x2, 0x35b, 0x35d, 0x7, 0x6, 0x2, 0x2, 0x35c, 0x35e, 
    0x7, 0x7c, 0x2, 0x2, 0x35d, 0x35c, 0x3, 0x2, 0x2, 0x2, 0x35d, 0x35e, 
    0x3, 0x2, 0x2, 0x2, 0x35e, 0x35f, 0x3, 0x2, 0x2, 0x2, 0x35f, 0x361, 
    0x5, 0x64, 0x33, 0x2, 0x360, 0x35b, 0x3, 0x2, 0x2, 0x2, 0x361, 0x364, 
    0x3, 0x2, 0x2, 0x2, 0x362, 0x360, 0x3, 0x2, 0x2, 0x2, 0x362, 0x363, 
    0x3, 0x2, 0x2, 0x2, 0x363, 0x5f, 0x3, 0x2, 0x2, 0x2, 0x364, 0x362, 0x3, 
    0x2, 0x2, 0x2, 0x365, 0x366, 0x7, 0x52, 0x2, 0x2, 0x366, 0x367, 0x7, 
    0x7c, 0x2, 0x2, 0x367, 0x368, 0x5, 0x86, 0x44, 0x2, 0x368, 0x61, 0x3, 
    0x2, 0x2, 0x2, 0x369, 0x36a, 0x7, 0x53, 0x2, 0x2, 0x36a, 0x36b, 0x7, 
    0x7c, 0x2, 0x2, 0x36b, 0x36c, 0x5, 0x86, 0x44, 0x2, 0x36c, 0x63, 0x3, 
    0x2, 0x2, 0x2, 0x36d, 0x372, 0x5, 0x86, 0x44, 0x2, 0x36e, 0x370, 0x7, 
    0x7c, 0x2, 0x2, 0x36f, 0x36e, 0x3, 0x2, 0x2, 0x2, 0x36f, 0x370, 0x3, 
    0x2, 0x2, 0x2, 0x370, 0x371, 0x3, 0x2, 0x2, 0x2, 0x371, 0x373, 0x9, 
    0x2, 0x2, 0x2, 0x372, 0x36f, 0x3, 0x2, 0x2, 0x2, 0x372, 0x373, 0x3, 
    0x2, 0x2, 0x2, 0x373, 0x65, 0x3, 0x2, 0x2, 0x2, 0x374, 0x375, 0x7, 0x58, 
    0x2, 0x2, 0x375, 0x376, 0x7, 0x7c, 0x2, 0x2, 0x376, 0x377, 0x5, 0x86, 
    0x44, 0x2, 0x377, 0x67, 0x3, 0x2, 0x2, 0x2, 0x378, 0x383, 0x5, 0x6a, 
    0x36, 0x2, 0x379, 0x37b, 0x7, 0x7c, 0x2, 0x2, 0x37a, 0x379, 0x3, 0x2, 
    0x2, 0x2, 0x37a, 0x37b, 0x3, 0x2, 0x2, 0x2, 0x37b, 0x37c, 0x3, 0x2, 
    0x2, 0x2, 0x37c, 0x37e, 0x7, 0x6, 0x2, 0x2, 0x37d, 0x37f, 0x7, 0x7c, 
    0x2, 0x2, 0x37e, 0x37d, 0x3, 0x2, 0x2, 0x2, 0x37e, 0x37f, 0x3, 0x2, 
    0x2, 0x2, 0x37f, 0x380, 0x3, 0x2, 0x2, 0x2, 0x380, 0x382, 0x5, 0x6a, 
    0x36, 0x2, 0x381, 0x37a, 0x3, 0x2, 0x2, 0x2, 0x382, 0x385, 0x3, 0x2, 
    0x2, 0x2, 0x383, 0x381, 0x3, 0x2, 0x2, 0x2, 0x383, 0x384, 0x3, 0x2, 
    0x2, 0x2, 0x384, 0x69, 0x3, 0x2, 0x2, 0x2, 0x385, 0x383, 0x3, 0x2, 0x2, 
    0x2, 0x386, 0x387, 0x5, 0x6c, 0x37, 0x2, 0x387, 0x6b, 0x3, 0x2, 0x2, 
    0x2, 0x388, 0x389, 0x5, 0x6e, 0x38, 0x2, 0x389, 0x6d, 0x3, 0x2, 0x2, 
    0x2, 0x38a, 0x391, 0x5, 0x70, 0x39, 0x2, 0x38b, 0x38d, 0x7, 0x7c, 0x2, 
    0x2, 0x38c, 0x38b, 0x3, 0x2, 0x2, 0x2, 0x38c, 0x38d, 0x3, 0x2, 0x2, 
    0x2, 0x38d, 0x38e, 0x3, 0x2, 0x2, 0x2, 0x38e, 0x390, 0x5, 0x72, 0x3a, 
    0x2, 0x38f, 0x38c, 0x3, 0x2, 0x2, 0x2, 0x390, 0x393, 0x3, 0x2, 0x2, 
    0x2, 0x391, 0x38f, 0x3, 0x2, 0x2, 0x2, 0x391, 0x392, 0x3, 0x2, 0x2, 
    0x2, 0x392, 0x399, 0x3, 0x2, 0x2, 0x2, 0x393, 0x391, 0x3, 0x2, 0x2, 
    0x2, 0x394, 0x395, 0x7, 0x4, 0x2, 0x2, 0x395, 0x396, 0x5, 0x6e, 0x38, 
    0x2, 0x396, 0x397, 0x7, 0x5, 0x2, 0x2, 0x397, 0x399, 0x3, 0x2, 0x2, 
    0x2, 0x398, 0x38a, 0x3, 0x2, 0x2, 0x2, 0x398, 0x394, 0x3, 0x2, 0x2, 
    0x2, 0x399, 0x6f, 0x3, 0x2, 0x2, 0x2, 0x39a, 0x39c, 0x7, 0x4, 0x2, 0x2, 
    0x39b, 0x39d, 0x7, 0x7c, 0x2, 0x2, 0x39c, 0x39b, 0x3, 0x2, 0x2, 0x2, 
    0x39c, 0x39d, 0x3, 0x2, 0x2, 0x2, 0x39d, 0x3a2, 0x3, 0x2, 0x2, 0x2, 
    0x39e, 0x3a0, 0x5, 0xd2, 0x6a, 0x2, 0x39f, 0x3a1, 0x7, 0x7c, 0x2, 0x2, 
    0x3a0, 0x39f, 0x3, 0x2, 0x2, 0x2, 0x3a0, 0x3a1, 0x3, 0x2, 0x2, 0x2, 
    0x3a1, 0x3a3, 0x3, 0x2, 0x2, 0x2, 0x3a2, 0x39e, 0x3, 0x2, 0x2, 0x2, 
    0x3a2, 0x3a3, 0x3, 0x2, 0x2, 0x2, 0x3a3, 0x3a8, 0x3, 0x2, 0x2, 0x2, 
    0x3a4, 0x3a6, 0x5, 0x7c, 0x3f, 0x2, 0x3a5, 0x3a7, 0x7, 0x7c, 0x2, 0x2, 
    0x3a6, 0x3a5, 0x3, 0x2, 0x2, 0x2, 0x3a6, 0x3a7, 0x3, 0x2, 0x2, 0x2, 
    0x3a7, 0x3a9, 0x3, 0x2, 0x2, 0x2, 0x3a8, 0x3a4, 0x3, 0x2, 0x2, 0x2, 
    0x3a8, 0x3a9, 0x3, 0x2, 0x2, 0x2, 0x3a9, 0x3ae, 0x3, 0x2, 0x2, 0x2, 
    0x3aa, 0x3ac, 0x5, 0x78, 0x3d, 0x2, 0x3ab, 0x3ad, 0x7, 0x7c, 0x2, 0x2, 
    0x3ac, 0x3ab, 0x3, 0x2, 0x2, 0x2, 0x3ac, 0x3ad, 0x3, 0x2, 0x2, 0x2, 
    0x3ad, 0x3af, 0x3, 0x2, 0x2, 0x2, 0x3ae, 0x3aa, 0x3, 0x2, 0x2, 0x2, 
    0x3ae, 0x3af, 0x3, 0x2, 0x2, 0x2, 0x3af, 0x3b0, 0x3, 0x2, 0x2, 0x2, 
    0x3b0, 0x3c8, 0x7, 0x5, 0x2, 0x2, 0x3b1, 0x3b3, 0x7, 0x7c, 0x2, 0x2, 
    0x3b2, 0x3b1, 0x3, 0x2, 0x2, 0x2, 0x3b2, 0x3b3, 0x3, 0x2, 0x2, 0x2, 
    0x3b3, 0x3b8, 0x3, 0x2, 0x2, 0x2, 0x3b4, 0x3b6, 0x5, 0xd2, 0x6a, 0x2, 
    0x3b5, 0x3b7, 0x7, 0x7c, 0x2, 0x2, 0x3b6, 0x3b5, 0x3, 0x2, 0x2, 0x2, 
    0x3b6, 0x3b7, 0x3, 0x2, 0x2, 0x2, 0x3b7, 0x3b9, 0x3, 0x2, 0x2, 0x2, 
    0x3b8, 0x3b4, 0x3, 0x2, 0x2, 0x2, 0x3b8, 0x3b9, 0x3, 0x2, 0x2, 0x2, 
    0x3b9, 0x3be, 0x3, 0x2, 0x2, 0x2, 0x3ba, 0x3bc, 0x5, 0x7c, 0x3f, 0x2, 
    0x3bb, 0x3bd, 0x7, 0x7c, 0x2, 0x2, 0x3bc, 0x3bb, 0x3, 0x2, 0x2, 0x2, 
    0x3bc, 0x3bd, 0x3, 0x2, 0x2, 0x2, 0x3bd, 0x3bf, 0x3, 0x2, 0x2, 0x2, 
    0x3be, 0x3ba, 0x3, 0x2, 0x2, 0x2, 0x3be, 0x3bf, 0x3, 0x2, 0x2, 0x2, 
    0x3bf, 0x3c4, 0x3, 0x2, 0x2, 0x2, 0x3c0, 0x3c2, 0x5, 0x78, 0x3d, 0x2, 
    0x3c1, 0x3c3, 0x7, 0x7c, 0x2, 0x2, 0x3c2, 0x3c1, 0x3, 0x2, 0x2, 0x2, 
    0x3c2, 0x3c3, 0x3, 0x2, 0x2, 0x2, 0x3c3, 0x3c5, 0x3, 0x2, 0x2, 0x2, 
    0x3c4, 0x3c0, 0x3, 0x2, 0x2, 0x2, 0x3c4, 0x3c5, 0x3, 0x2, 0x2, 0x2, 
    0x3c5, 0x3c6, 0x3, 0x2, 0x2, 0x2, 0x3c6, 0x3c8, 0x8, 0x39, 0x1, 0x2, 
    0x3c7, 0x39a, 0x3, 0x2, 0x2, 0x2, 0x3c7, 0x3b2, 0x3, 0x2, 0x2, 0x2, 
    0x3c8, 0x71, 0x3, 0x2, 0x2, 0x2, 0x3c9, 0x3cb, 0x5, 0x74, 0x3b, 0x2, 
    0x3ca, 0x3cc, 0x7, 0x7c, 0x2, 0x2, 0x3cb, 0x3ca, 0x3, 0x2, 0x2, 0x2, 
    0x3cb, 0x3cc, 0x3, 0x2, 0x2, 0x2, 0x3cc, 0x3cd, 0x3, 0x2, 0x2, 0x2, 
    0x3cd, 0x3ce, 0x5, 0x70, 0x39, 0x2, 0x3ce, 0x73, 0x3, 0x2, 0x2, 0x2, 
    0x3cf, 0x3d1, 0x5, 0xe4, 0x73, 0x2, 0x3d0, 0x3d2, 0x7, 0x7c, 0x2, 0x2, 
    0x3d1, 0x3d0, 0x3, 0x2, 0x2, 0x2, 0x3d1, 0x3d2, 0x3, 0x2, 0x2, 0x2, 
    0x3d2, 0x3d3, 0x3, 0x2, 0x2, 0x2, 0x3d3, 0x3d5, 0x5, 0xe8, 0x75, 0x2, 
    0x3d4, 0x3d6, 0x7, 0x7c, 0x2, 0x2, 0x3d5, 0x3d4, 0x3, 0x2, 0x2, 0x2, 
    0x3d5, 0x3d6, 0x3, 0x2, 0x2, 0x2, 0x3d6, 0x3d8, 0x3, 0x2, 0x2, 0x2, 
    0x3d7, 0x3d9, 0x5, 0x76, 0x3c, 0x2, 0x3d8, 0x3d7, 0x3, 0x2, 0x2, 0x2, 
    0x3d8, 0x3d9, 0x3, 0x2, 0x2, 0x2, 0x3d9, 0x3db, 0x3, 0x2, 0x2, 0x2, 
    0x3da, 0x3dc, 0x7, 0x7c, 0x2, 0x2, 0x3db, 0x3da, 0x3, 0x2, 0x2, 0x2, 
    0x3db, 0x3dc, 0x3, 0x2, 0x2, 0x2, 0x3dc, 0x3dd, 0x3, 0x2, 0x2, 0x2, 
    0x3dd, 0x3de, 0x5, 0xe8, 0x75, 0x2, 0x3de, 0x3fc, 0x3, 0x2, 0x2, 0x2, 
    0x3df, 0x3e1, 0x5, 0xe8, 0x75, 0x2, 0x3e0, 0x3e2, 0x7, 0x7c, 0x2, 0x2, 
    0x3e1, 0x3e0, 0x3, 0x2, 0x2, 0x2, 0x3e1, 0x3e2, 0x3, 0x2, 0x2, 0x2, 
    0x3e2, 0x3e4, 0x3, 0x2, 0x2, 0x2, 0x3e3, 0x3e5, 0x5, 0x76, 0x3c, 0x2, 
    0x3e4, 0x3e3, 0x3, 0x2, 0x2, 0x2, 0x3e4, 0x3e5, 0x3, 0x2, 0x2, 0x2, 
    0x3e5, 0x3e7, 0x3, 0x2, 0x2, 0x2, 0x3e6, 0x3e8, 0x7, 0x7c, 0x2, 0x2, 
    0x3e7, 0x3e6, 0x3, 0x2, 0x2, 0x2, 0x3e7, 0x3e8, 0x3, 0x2, 0x2, 0x2, 
    0x3e8, 0x3e9, 0x3, 0x2, 0x2, 0x2, 0x3e9, 0x3eb, 0x5, 0xe8, 0x75, 0x2, 
    0x3ea, 0x3ec, 0x7, 0x7c, 0x2, 0x2, 0x3eb, 0x3ea, 0x3, 0x2, 0x2, 0x2, 
    0x3eb, 0x3ec, 0x3, 0x2, 0x2, 0x2, 0x3ec, 0x3ed, 0x3, 0x2, 0x2, 0x2, 
    0x3ed, 0x3ee, 0x5, 0xe6, 0x74, 0x2, 0x3ee, 0x3fc, 0x3, 0x2, 0x2, 0x2, 
    0x3ef, 0x3f1, 0x5, 0xe8, 0x75, 0x2, 0x3f0, 0x3f2, 0x7, 0x7c, 0x2, 0x2, 
    0x3f1, 0x3f0, 0x3, 0x2, 0x2, 0x2, 0x3f1, 0x3f2, 0x3, 0x2, 0x2, 0x2, 
    0x3f2, 0x3f4, 0x3, 0x2, 0x2, 0x2, 0x3f3, 0x3f5, 0x5, 0x76, 0x3c, 0x2, 
    0x3f4, 0x3f3, 0x3, 0x2, 0x2, 0x2, 0x3f4, 0x3f5, 0x3, 0x2, 0x2, 0x2, 
    0x3f5, 0x3f7, 0x3, 0x2, 0x2, 0x2, 0x3f6, 0x3f8, 0x7, 0x7c, 0x2, 0x2, 
    0x3f7, 0x3f6, 0x3, 0x2, 0x2, 0x2, 0x3f7, 0x3f8, 0x3, 0x2, 0x2, 0x2, 
    0x3f8, 0x3f9, 0x3, 0x2, 0x2, 0x2, 0x3f9, 0x3fa, 0x5, 0xe8, 0x75, 0x2, 
    0x3fa, 0x3fc, 0x3, 0x2, 0x2, 0x2, 0x3fb, 0x3cf, 0x3, 0x2, 0x2, 0x2, 
    0x3fb, 0x3df, 0x3, 0x2, 0x2, 0x2, 0x3fb, 0x3ef, 0x3, 0x2, 0x2, 0x2, 
    0x3fc, 0x75, 0x3, 0x2, 0x2, 0x2, 0x3fd, 0x3ff, 0x7, 0x7, 0x2, 0x2, 0x3fe, 
    0x400, 0x7, 0x7c, 0x2, 0x2, 0x3ff, 0x3fe, 0x3, 0x2, 0x2, 0x2, 0x3ff, 
    0x400, 0x3, 0x2, 0x2, 0x2, 0x400, 0x405, 0x3, 0x2, 0x2, 0x2, 0x401, 
    0x403, 0x5, 0xd2, 0x6a, 0x2, 0x402, 0x404, 0x7, 0x7c, 0x2, 0x2, 0x403, 
    0x402, 0x3, 0x2, 0x2, 0x2, 0x403, 0x404, 0x3, 0x2, 0x2, 0x2, 0x404, 
    0x406, 0x3, 0x2, 0x2, 0x2, 0x405, 0x401, 0x3, 0x2, 0x2, 0x2, 0x405, 
    0x406, 0x3, 0x2, 0x2, 0x2, 0x406, 0x40b, 0x3, 0x2, 0x2, 0x2, 0x407, 
    0x409, 0x5, 0x7a, 0x3e, 0x2, 0x408, 0x40a, 0x7, 0x7c, 0x2, 0x2, 0x409, 
    0x408, 0x3, 0x2, 0x2, 0x2, 0x409, 0x40a, 0x3, 0x2, 0x2, 0x2, 0x40a, 
    0x40c, 0x3, 0x2, 0x2, 0x2, 0x40b, 0x407, 0x3, 0x2, 0x2, 0x2, 0x40b, 
    0x40c, 0x3, 0x2, 0x2, 0x2, 0x40c, 0x411, 0x3, 0x2, 0x2, 0x2, 0x40d, 
    0x40f, 0x5, 0x80, 0x41, 0x2, 0x40e, 0x410, 0x7, 0x7c, 0x2, 0x2, 0x40f, 
    0x40e, 0x3, 0x2, 0x2, 0x2, 0x40f, 0x410, 0x3, 0x2, 0x2, 0x2, 0x410, 
    0x412, 0x3, 0x2, 0x2, 0x2, 0x411, 0x40d, 0x3, 0x2, 0x2, 0x2, 0x411, 
    0x412, 0x3, 0x2, 0x2, 0x2, 0x412, 0x417, 0x3, 0x2, 0x2, 0x2, 0x413, 
    0x415, 0x5, 0x78, 0x3d, 0x2, 0x414, 0x416, 0x7, 0x7c, 0x2, 0x2, 0x415, 
    0x414, 0x3, 0x2, 0x2, 0x2, 0x415, 0x416, 0x3, 0x2, 0x2, 0x2, 0x416, 
    0x418, 0x3, 0x2, 0x2, 0x2, 0x417, 0x413, 0x3, 0x2, 0x2, 0x2, 0x417, 
    0x418, 0x3, 0x2, 0x2, 0x2, 0x418, 0x419, 0x3, 0x2, 0x2, 0x2, 0x419, 
    0x41a, 0x7, 0x8, 0x2, 0x2, 0x41a, 0x77, 0x3, 0x2, 0x2, 0x2, 0x41b, 0x41d, 
    0x7, 0xa, 0x2, 0x2, 0x41c, 0x41e, 0x7, 0x7c, 0x2, 0x2, 0x41d, 0x41c, 
    0x3, 0x2, 0x2, 0x2, 0x41d, 0x41e, 0x3, 0x2, 0x2, 0x2, 0x41e, 0x440, 
    0x3, 0x2, 0x2, 0x2, 0x41f, 0x421, 0x5, 0xda, 0x6e, 0x2, 0x420, 0x422, 
    0x7, 0x7c, 0x2, 0x2, 0x421, 0x420, 0x3, 0x2, 0x2, 0x2, 0x421, 0x422, 
    0x3, 0x2, 0x2, 0x2, 0x422, 0x423, 0x3, 0x2, 0x2, 0x2, 0x423, 0x425, 
    0x7, 0xb, 0x2, 0x2, 0x424, 0x426, 0x7, 0x7c, 0x2, 0x2, 0x425, 0x424, 
    0x3, 0x2, 0x2, 0x2, 0x425, 0x426, 0x3, 0x2, 0x2, 0x2, 0x426, 0x427, 
    0x3, 0x2, 0x2, 0x2, 0x427, 0x429, 0x5, 0x86, 0x44, 0x2, 0x428, 0x42a, 
    0x7, 0x7c, 0x2, 0x2, 0x429, 0x428, 0x3, 0x2, 0x2, 0x2, 0x429, 0x42a, 
    0x3, 0x2, 0x2, 0x2, 0x42a, 0x43d, 0x3, 0x2, 0x2, 0x2, 0x42b, 0x42d, 
    0x7, 0x6, 0x2, 0x2, 0x42c, 0x42e, 0x7, 0x7c, 0x2, 0x2, 0x42d, 0x42c, 
    0x3, 0x2, 0x2, 0x2, 0x42d, 0x42e, 0x3, 0x2, 0x2, 0x2, 0x42e, 0x42f, 
    0x3, 0x2, 0x2, 0x2, 0x42f, 0x431, 0x5, 0xda, 0x6e, 0x2, 0x430, 0x432, 
    0x7, 0x7c, 0x2, 0x2, 0x431, 0x430, 0x3, 0x2, 0x2, 0x2, 0x431, 0x432, 
    0x3, 0x2, 0x2, 0x2, 0x432, 0x433, 0x3, 0x2, 0x2, 0x2, 0x433, 0x435, 
    0x7, 0xb, 0x2, 0x2, 0x434, 0x436, 0x7, 0x7c, 0x2, 0x2, 0x435, 0x434, 
    0x3, 0x2, 0x2, 0x2, 0x435, 0x436, 0x3, 0x2, 0x2, 0x2, 0x436, 0x437, 
    0x3, 0x2, 0x2, 0x2, 0x437, 0x439, 0x5, 0x86, 0x44, 0x2, 0x438, 0x43a, 
    0x7, 0x7c, 0x2, 0x2, 0x439, 0x438, 0x3, 0x2, 0x2, 0x2, 0x439, 0x43a, 
    0x3, 0x2, 0x2, 0x2, 0x43a, 0x43c, 0x3, 0x2, 0x2, 0x2, 0x43b, 0x42b, 
    0x3, 0x2, 0x2, 0x2, 0x43c, 0x43f, 0x3, 0x2, 0x2, 0x2, 0x43d, 0x43b, 
    0x3, 0x2, 0x2, 0x2, 0x43d, 0x43e, 0x3, 0x2, 0x2, 0x2, 0x43e, 0x441, 
    0x3, 0x2, 0x2, 0x2, 0x43f, 0x43d, 0x3, 0x2, 0x2, 0x2, 0x440, 0x41f, 
    0x3, 0x2, 0x2, 0x2, 0x440, 0x441, 0x3, 0x2, 0x2, 0x2, 0x441, 0x442, 
    0x3, 0x2, 0x2, 0x2, 0x442, 0x443, 0x7, 0xc, 0x2, 0x2, 0x443, 0x79, 0x3, 
    0x2, 0x2, 0x2, 0x444, 0x446, 0x7, 0xb, 0x2, 0x2, 0x445, 0x447, 0x7, 
    0x7c, 0x2, 0x2, 0x446, 0x445, 0x3, 0x2, 0x2, 0x2, 0x446, 0x447, 0x3, 
    0x2, 0x2, 0x2, 0x447, 0x448, 0x3, 0x2, 0x2, 0x2, 0x448, 0x456, 0x5, 
    0x84, 0x43, 0x2, 0x449, 0x44b, 0x7, 0x7c, 0x2, 0x2, 0x44a, 0x449, 0x3, 
    0x2, 0x2, 0x2, 0x44a, 0x44b, 0x3, 0x2, 0x2, 0x2, 0x44b, 0x44c, 0x3, 
    0x2, 0x2, 0x2, 0x44c, 0x44e, 0x7, 0xd, 0x2, 0x2, 0x44d, 0x44f, 0x7, 
    0xb, 0x2, 0x2, 0x44e, 0x44d, 0x3, 0x2, 0x2, 0x2, 0x44e, 0x44f, 0x3, 
    0x2, 0x2, 0x2, 0x44f, 0x451, 0x3, 0x2, 0x2, 0x2, 0x450, 0x452, 0x7, 
    0x7c, 0x2, 0x2, 0x451, 0x450, 0x3, 0x2, 0x2, 0x2, 0x451, 0x452, 0x3, 
    0x2, 0x2, 0x2, 0x452, 0x453, 0x3, 0x2, 0x2, 0x2, 0x453, 0x455, 0x5, 
    0x84, 0x43, 0x2, 0x454, 0x44a, 0x3, 0x2, 0x2, 0x2, 0x455, 0x458, 0x3, 
    0x2, 0x2, 0x2, 0x456, 0x454, 0x3, 0x2, 0x2, 0x2, 0x456, 0x457, 0x3, 
    0x2, 0x2, 0x2, 0x457, 0x7b, 0x3, 0x2, 0x2, 0x2, 0x458, 0x456, 0x3, 0x2, 
    0x2, 0x2, 0x459, 0x460, 0x5, 0x7e, 0x40, 0x2, 0x45a, 0x45c, 0x7, 0x7c, 
    0x2, 0x2, 0x45b, 0x45a, 0x3, 0x2, 0x2, 0x2, 0x45b, 0x45c, 0x3, 0x2, 
    0x2, 0x2, 0x45c, 0x45d, 0x3, 0x2, 0x2, 0x2, 0x45d, 0x45f, 0x5, 0x7e, 
    0x40, 0x2, 0x45e, 0x45b, 0x3, 0x2, 0x2, 0x2, 0x45f, 0x462, 0x3, 0x2, 
    0x2, 0x2, 0x460, 0x45e, 0x3, 0x2, 0x2, 0x2, 0x460, 0x461, 0x3, 0x2, 
    0x2, 0x2, 0x461, 0x7d, 0x3, 0x2, 0x2, 0x2, 0x462, 0x460, 0x3, 0x2, 0x2, 
    0x2, 0x463, 0x465, 0x7, 0xb, 0x2, 0x2, 0x464, 0x466, 0x7, 0x7c, 0x2, 
    0x2, 0x465, 0x464, 0x3, 0x2, 0x2, 0x2, 0x465, 0x466, 0x3, 0x2, 0x2, 
    0x2, 0x466, 0x467, 0x3, 0x2, 0x2, 0x2, 0x467, 0x468, 0x5, 0x82, 0x42, 
    0x2, 0x468, 0x7f, 0x3, 0x2, 0x2, 0x2, 0x469, 0x46b, 0x7, 0x4e, 0x2, 
    0x2, 0x46a, 0x46c, 0x7, 0x7c, 0x2, 0x2, 0x46b, 0x46a, 0x3, 0x2, 0x2, 
    0x2, 0x46b, 0x46c, 0x3, 0x2, 0x2, 0x2, 0x46c, 0x46e, 0x3, 0x2, 0x2, 
    0x2, 0x46d, 0x46f, 0x7, 0x59, 0x2, 0x2, 0x46e, 0x46d, 0x3, 0x2, 0x2, 
    0x2, 0x46e, 0x46f, 0x3, 0x2, 0x2, 0x2, 0x46f, 0x471, 0x3, 0x2, 0x2, 
    0x2, 0x470, 0x472, 0x7, 0x7c, 0x2, 0x2, 0x471, 0x470, 0x3, 0x2, 0x2, 
    0x2, 0x471, 0x472, 0x3, 0x2, 0x2, 0x2, 0x472, 0x473, 0x3, 0x2, 0x2, 
    0x2, 0x473, 0x475, 0x5, 0xdc, 0x6f, 0x2, 0x474, 0x476, 0x7, 0x7c, 0x2, 
    0x2, 0x475, 0x474, 0x3, 0x2, 0x2, 0x2, 0x475, 0x476, 0x3, 0x2, 0x2, 
    0x2, 0x476, 0x477, 0x3, 0x2, 0x2, 0x2, 0x477, 0x479, 0x7, 0xe, 0x2, 
    0x2, 0x478, 0x47a, 0x7, 0x7c, 0x2, 0x2, 0x479, 0x478, 0x3, 0x2, 0x2, 
    0x2, 0x479, 0x47a, 0x3, 0x2, 0x2, 0x2, 0x47a, 0x47b, 0x3, 0x2, 0x2, 
    0x2, 0x47b, 0x47c, 0x5, 0xdc, 0x6f, 0x2, 0x47c, 0x81, 0x3, 0x2, 0x2, 
    0x2, 0x47d, 0x47e, 0x5, 0xe0, 0x71, 0x2, 0x47e, 0x83, 0x3, 0x2, 0x2, 
    0x2, 0x47f, 0x480, 0x5, 0xe0, 0x71, 0x2, 0x480, 0x85, 0x3, 0x2, 0x2, 
    0x2, 0x481, 0x482, 0x5, 0x88, 0x45, 0x2, 0x482, 0x87, 0x3, 0x2, 0x2, 
    0x2, 0x483, 0x48a, 0x5, 0x8a, 0x46, 0x2, 0x484, 0x485, 0x7, 0x7c, 0x2, 
    0x2, 0x485, 0x486, 0x7, 0x5a, 0x2, 0x2, 0x486, 0x487, 0x7, 0x7c, 0x2, 
    0x2, 0x487, 0x489, 0x5, 0x8a, 0x46, 0x2, 0x488, 0x484, 0x3, 0x2, 0x2, 
    0x2, 0x489, 0x48c, 0x3, 0x2, 0x2, 0x2, 0x48a, 0x488, 0x3, 0x2, 0x2, 
    0x2, 0x48a, 0x48b, 0x3, 0x2, 0x2, 0x2, 0x48b, 0x89, 0x3, 0x2, 0x2, 0x2, 
    0x48c, 0x48a, 0x3, 0x2, 0x2, 0x2, 0x48d, 0x494, 0x5, 0x8c, 0x47, 0x2, 
    0x48e, 0x48f, 0x7, 0x7c, 0x2, 0x2, 0x48f, 0x490, 0x7, 0x5b, 0x2, 0x2, 
    0x490, 0x491, 0x7, 0x7c, 0x2, 0x2, 0x491, 0x493, 0x5, 0x8c, 0x47, 0x2, 
    0x492, 0x48e, 0x3, 0x2, 0x2, 0x2, 0x493, 0x496, 0x3, 0x2, 0x2, 0x2, 
    0x494, 0x492, 0x3, 0x2, 0x2, 0x2, 0x494, 0x495, 0x3, 0x2, 0x2, 0x2, 
    0x495, 0x8b, 0x3, 0x2, 0x2, 0x2, 0x496, 0x494, 0x3, 0x2, 0x2, 0x2, 0x497, 
    0x49e, 0x5, 0x8e, 0x48, 0x2, 0x498, 0x499, 0x7, 0x7c, 0x2, 0x2, 0x499, 
    0x49a, 0x7, 0x5c, 0x2, 0x2, 0x49a, 0x49b, 0x7, 0x7c, 0x2, 0x2, 0x49b, 
    0x49d, 0x5, 0x8e, 0x48, 0x2, 0x49c, 0x498, 0x3, 0x2, 0x2, 0x2, 0x49d, 
    0x4a0, 0x3, 0x2, 0x2, 0x2, 0x49e, 0x49c, 0x3, 0x2, 0x2, 0x2, 0x49e, 
    0x49f, 0x3, 0x2, 0x2, 0x2, 0x49f, 0x8d, 0x3, 0x2, 0x2, 0x2, 0x4a0, 0x49e, 
    0x3, 0x2, 0x2, 0x2, 0x4a1, 0x4a3, 0x7, 0x5d, 0x2, 0x2, 0x4a2, 0x4a4, 
    0x7, 0x7c, 0x2, 0x2, 0x4a3, 0x4a2, 0x3, 0x2, 0x2, 0x2, 0x4a3, 0x4a4, 
    0x3, 0x2, 0x2, 0x2, 0x4a4, 0x4a6, 0x3, 0x2, 0x2, 0x2, 0x4a5, 0x4a1, 
    0x3, 0x2, 0x2, 0x2, 0x4a5, 0x4a6, 0x3, 0x2, 0x2, 0x2, 0x4a6, 0x4a7, 
    0x3, 0x2, 0x2, 0x2, 0x4a7, 0x4a8, 0x5, 0x90, 0x49, 0x2, 0x4a8, 0x8f, 
    0x3, 0x2, 0x2, 0x2, 0x4a9, 0x4b3, 0x5, 0x94, 0x4b, 0x2, 0x4aa, 0x4ac, 
    0x7, 0x7c, 0x2, 0x2, 0x4ab, 0x4aa, 0x3, 0x2, 0x2, 0x2, 0x4ab, 0x4ac, 
    0x3, 0x2, 0x2, 0x2, 0x4ac, 0x4ad, 0x3, 0x2, 0x2, 0x2, 0x4ad, 0x4af, 
    0x5, 0x92, 0x4a, 0x2, 0x4ae, 0x4b0, 0x7, 0x7c, 0x2, 0x2, 0x4af, 0x4ae, 
    0x3, 0x2, 0x2, 0x2, 0x4af, 0x4b0, 0x3, 0x2, 0x2, 0x2, 0x4b0, 0x4b1, 
    0x3, 0x2, 0x2, 0x2, 0x4b1, 0x4b2, 0x5, 0x94, 0x4b, 0x2, 0x4b2, 0x4b4, 
    0x3, 0x2, 0x2, 0x2, 0x4b3, 0x4ab, 0x3, 0x2, 0x2, 0x2, 0x4b3, 0x4b4, 
    0x3, 0x2, 0x2, 0x2, 0x4b4, 0x4da, 0x3, 0x2, 0x2, 0x2, 0x4b5, 0x4b7, 
    0x5, 0x94, 0x4b, 0x2, 0x4b6, 0x4b8, 0x7, 0x7c, 0x2, 0x2, 0x4b7, 0x4b6, 
    0x3, 0x2, 0x2, 0x2, 0x4b7, 0x4b8, 0x3, 0x2, 0x2, 0x2, 0x4b8, 0x4b9, 
    0x3, 0x2, 0x2, 0x2, 0x4b9, 0x4bb, 0x7, 0x5e, 0x2, 0x2, 0x4ba, 0x4bc, 
    0x7, 0x7c, 0x2, 0x2, 0x4bb, 0x4ba, 0x3, 0x2, 0x2, 0x2, 0x4bb, 0x4bc, 
    0x3, 0x2, 0x2, 0x2, 0x4bc, 0x4bd, 0x3, 0x2, 0x2, 0x2, 0x4bd, 0x4be, 
    0x5, 0x94, 0x4b, 0x2, 0x4be, 0x4bf, 0x3, 0x2, 0x2, 0x2, 0x4bf, 0x4c0, 
    0x8, 0x49, 0x1, 0x2, 0x4c0, 0x4da, 0x3, 0x2, 0x2, 0x2, 0x4c1, 0x4c3, 
    0x5, 0x94, 0x4b, 0x2, 0x4c2, 0x4c4, 0x7, 0x7c, 0x2, 0x2, 0x4c3, 0x4c2, 
    0x3, 0x2, 0x2, 0x2, 0x4c3, 0x4c4, 0x3, 0x2, 0x2, 0x2, 0x4c4, 0x4c5, 
    0x3, 0x2, 0x2, 0x2, 0x4c5, 0x4c7, 0x5, 0x92, 0x4a, 0x2, 0x4c6, 0x4c8, 
    0x7, 0x7c, 0x2, 0x2, 0x4c7, 0x4c6, 0x3, 0x2, 0x2, 0x2, 0x4c7, 0x4c8, 
    0x3, 0x2, 0x2, 0x2, 0x4c8, 0x4c9, 0x3, 0x2, 0x2, 0x2, 0x4c9, 0x4d3, 
    0x5, 0x94, 0x4b, 0x2, 0x4ca, 0x4cc, 0x7, 0x7c, 0x2, 0x2, 0x4cb, 0x4ca, 
    0x3, 0x2, 0x2, 0x2, 0x4cb, 0x4cc, 0x3, 0x2, 0x2, 0x2, 0x4cc, 0x4cd, 
    0x3, 0x2, 0x2, 0x2, 0x4cd, 0x4cf, 0x5, 0x92, 0x4a, 0x2, 0x4ce, 0x4d0, 
    0x7, 0x7c, 0x2, 0x2, 0x4cf, 0x4ce, 0x3, 0x2, 0x2, 0x2, 0x4cf, 0x4d0, 
    0x3, 0x2, 0x2, 0x2, 0x4d0, 0x4d1, 0x3, 0x2, 0x2, 0x2, 0x4d1, 0x4d2, 
    0x5, 0x94, 0x4b, 0x2, 0x4d2, 0x4d4, 0x3, 0x2, 0x2, 0x2, 0x4d3, 0x4cb, 
    0x3, 0x2, 0x2, 0x2, 0x4d4, 0x4d5, 0x3, 0x2, 0x2, 0x2, 0x4d5, 0x4d3, 
    0x3, 0x2, 0x2, 0x2, 0x4d5, 0x4d6, 0x3, 0x2, 0x2, 0x2, 0x4d6, 0x4d7, 
    0x3, 0x2, 0x2, 0x2, 0x4d7, 0x4d8, 0x8, 0x49, 0x1, 0x2, 0x4d8, 0x4da, 
    0x3, 0x2, 0x2, 0x2, 0x4d9, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x4d9, 0x4b5, 
    0x3, 0x2, 0x2, 0x2, 0x4d9, 0x4c1, 0x3, 0x2, 0x2, 0x2, 0x4da, 0x91, 0x3, 
    0x2, 0x2, 0x2, 0x4db, 0x4dc, 0x9, 0x3, 0x2, 0x2, 0x4dc, 0x93, 0x3, 0x2, 
    0x2, 0x2, 0x4dd, 0x4e8, 0x5, 0x96, 0x4c, 0x2, 0x4de, 0x4e0, 0x7, 0x7c, 
    0x2, 0x2, 0x4df, 0x4de, 0x3, 0x2, 0x2, 0x2, 0x4df, 0x4e0, 0x3, 0x2, 
    0x2, 0x2, 0x4e0, 0x4e1, 0x3, 0x2, 0x2, 0x2, 0x4e1, 0x4e3, 0x7, 0xd, 
    0x2, 0x2, 0x4e2, 0x4e4, 0x7, 0x7c, 0x2, 0x2, 0x4e3, 0x4e2, 0x3, 0x2, 
    0x2, 0x2, 0x4e3, 0x4e4, 0x3, 0x2, 0x2, 0x2, 0x4e4, 0x4e5, 0x3, 0x2, 
    0x2, 0x2, 0x4e5, 0x4e7, 0x5, 0x96, 0x4c, 0x2, 0x4e6, 0x4df, 0x3, 0x2, 
    0x2, 0x2, 0x4e7, 0x4ea, 0x3, 0x2, 0x2, 0x2, 0x4e8, 0x4e6, 0x3, 0x2, 
    0x2, 0x2, 0x4e8, 0x4e9, 0x3, 0x2, 0x2, 0x2, 0x4e9, 0x95, 0x3, 0x2, 0x2, 
    0x2, 0x4ea, 0x4e8, 0x3, 0x2, 0x2, 0x2, 0x4eb, 0x4f6, 0x5, 0x98, 0x4d, 
    0x2, 0x4ec, 0x4ee, 0x7, 0x7c, 0x2, 0x2, 0x4ed, 0x4ec, 0x3, 0x2, 0x2, 
    0x2, 0x4ed, 0x4ee, 0x3, 0x2, 0x2, 0x2, 0x4ee, 0x4ef, 0x3, 0x2, 0x2, 
    0x2, 0x4ef, 0x4f1, 0x7, 0x14, 0x2, 0x2, 0x4f0, 0x4f2, 0x7, 0x7c, 0x2, 
    0x2, 0x4f1, 0x4f0, 0x3, 0x2, 0x2, 0x2, 0x4f1, 0x4f2, 0x3, 0x2, 0x2, 
    0x2, 0x4f2, 0x4f3, 0x3, 0x2, 0x2, 0x2, 0x4f3, 0x4f5, 0x5, 0x98, 0x4d, 
    0x2, 0x4f4, 0x4ed, 0x3, 0x2, 0x2, 0x2, 0x4f5, 0x4f8, 0x3, 0x2, 0x2, 
    0x2, 0x4f6, 0x4f4, 0x3, 0x2, 0x2, 0x2, 0x4f6, 0x4f7, 0x3, 0x2, 0x2, 
    0x2, 0x4f7, 0x97, 0x3, 0x2, 0x2, 0x2, 0x4f8, 0x4f6, 0x3, 0x2, 0x2, 0x2, 
    0x4f9, 0x505, 0x5, 0x9c, 0x4f, 0x2, 0x4fa, 0x4fc, 0x7, 0x7c, 0x2, 0x2, 
    0x4fb, 0x4fa, 0x3, 0x2, 0x2, 0x2, 0x4fb, 0x4fc, 0x3, 0x2, 0x2, 0x2, 
    0x4fc, 0x4fd, 0x3, 0x2, 0x2, 0x2, 0x4fd, 0x4ff, 0x5, 0x9a, 0x4e, 0x2, 
    0x4fe, 0x500, 0x7, 0x7c, 0x2, 0x2, 0x4ff, 0x4fe, 0x3, 0x2, 0x2, 0x2, 
    0x4ff, 0x500, 0x3, 0x2, 0x2, 0x2, 0x500, 0x501, 0x3, 0x2, 0x2, 0x2, 
    0x501, 0x502, 0x5, 0x9c, 0x4f, 0x2, 0x502, 0x504, 0x3, 0x2, 0x2, 0x2, 
    0x503, 0x4fb, 0x3, 0x2, 0x2, 0x2, 0x504, 0x507, 0x3, 0x2, 0x2, 0x2, 
    0x505, 0x503, 0x3, 0x2, 0x2, 0x2, 0x505, 0x506, 0x3, 0x2, 0x2, 0x2, 
    0x506, 0x99, 0x3, 0x2, 0x2, 0x2, 0x507, 0x505, 0x3, 0x2, 0x2, 0x2, 0x508, 
    0x509, 0x9, 0x4, 0x2, 0x2, 0x509, 0x9b, 0x3, 0x2, 0x2, 0x2, 0x50a, 0x516, 
    0x5, 0xa0, 0x51, 0x2, 0x50b, 0x50d, 0x7, 0x7c, 0x2, 0x2, 0x50c, 0x50b, 
    0x3, 0x2, 0x2, 0x2, 0x50c, 0x50d, 0x3, 0x2, 0x2, 0x2, 0x50d, 0x50e, 
    0x3, 0x2, 0x2, 0x2, 0x50e, 0x510, 0x5, 0x9e, 0x50, 0x2, 0x50f, 0x511, 
    0x7, 0x7c, 0x2, 0x2, 0x510, 0x50f, 0x3, 0x2, 0x2, 0x2, 0x510, 0x511, 
    0x3, 0x2, 0x2, 0x2, 0x511, 0x512, 0x3, 0x2, 0x2, 0x2, 0x512, 0x513, 
    0x5, 0xa0, 0x51, 0x2, 0x513, 0x515, 0x3, 0x2, 0x2, 0x2, 0x514, 0x50c, 
    0x3, 0x2, 0x2, 0x2, 0x515, 0x518, 0x3, 0x2, 0x2, 0x2, 0x516, 0x514, 
    0x3, 0x2, 0x2, 0x2, 0x516, 0x517, 0x3, 0x2, 0x2, 0x2, 0x517, 0x9d, 0x3, 
    0x2, 0x2, 0x2, 0x518, 0x516, 0x3, 0x2, 0x2, 0x2, 0x519, 0x51a, 0x9, 
    0x5, 0x2, 0x2, 0x51a, 0x9f, 0x3, 0x2, 0x2, 0x2, 0x51b, 0x527, 0x5, 0xa4, 
    0x53, 0x2, 0x51c, 0x51e, 0x7, 0x7c, 0x2, 0x2, 0x51d, 0x51c, 0x3, 0x2, 
    0x2, 0x2, 0x51d, 0x51e, 0x3, 0x2, 0x2, 0x2, 0x51e, 0x51f, 0x3, 0x2, 
    0x2, 0x2, 0x51f, 0x521, 0x5, 0xa2, 0x52, 0x2, 0x520, 0x522, 0x7, 0x7c, 
    0x2, 0x2, 0x521, 0x520, 0x3, 0x2, 0x2, 0x2, 0x521, 0x522, 0x3, 0x2, 
    0x2, 0x2, 0x522, 0x523, 0x3, 0x2, 0x2, 0x2, 0x523, 0x524, 0x5, 0xa4, 
    0x53, 0x2, 0x524, 0x526, 0x3, 0x2, 0x2, 0x2, 0x525, 0x51d, 0x3, 0x2, 
    0x2, 0x2, 0x526, 0x529, 0x3, 0x2, 0x2, 0x2, 0x527, 0x525, 0x3, 0x2, 
    0x2, 0x2, 0x527, 0x528, 0x3, 0x2, 0x2, 0x2, 0x528, 0xa1, 0x3, 0x2, 0x2, 
    0x2, 0x529, 0x527, 0x3, 0x2, 0x2, 0x2, 0x52a, 0x52b, 0x9, 0x6, 0x2, 
    0x2, 0x52b, 0xa3, 0x3, 0x2, 0x2, 0x2, 0x52c, 0x537, 0x5, 0xa6, 0x54, 
    0x2, 0x52d, 0x52f, 0x7, 0x7c, 0x2, 0x2, 0x52e, 0x52d, 0x3, 0x2, 0x2, 
    0x2, 0x52e, 0x52f, 0x3, 0x2, 0x2, 0x2, 0x52f, 0x530, 0x3, 0x2, 0x2, 
    0x2, 0x530, 0x532, 0x7, 0x1a, 0x2, 0x2, 0x531, 0x533, 0x7, 0x7c, 0x2, 
    0x2, 0x532, 0x531, 0x3, 0x2, 0x2, 0x2, 0x532, 0x533, 0x3, 0x2, 0x2, 
    0x2, 0x533, 0x534, 0x3, 0x2, 0x2, 0x2, 0x534, 0x536, 0x5, 0xa6, 0x54, 
    0x2, 0x535, 0x52e, 0x3, 0x2, 0x2, 0x2, 0x536, 0x539, 0x3, 0x2, 0x2, 
    0x2, 0x537, 0x535, 0x3, 0x2, 0x2, 0x2, 0x537, 0x538, 0x3, 0x2, 0x2, 
    0x2, 0x538, 0xa5, 0x3, 0x2, 0x2, 0x2, 0x539, 0x537, 0x3, 0x2, 0x2, 0x2, 
    0x53a, 0x53c, 0x7, 0x5f, 0x2, 0x2, 0x53b, 0x53d, 0x7, 0x7c, 0x2, 0x2, 
    0x53c, 0x53b, 0x3, 0x2, 0x2, 0x2, 0x53c, 0x53d, 0x3, 0x2, 0x2, 0x2, 
    0x53d, 0x53f, 0x3, 0x2, 0x2, 0x2, 0x53e, 0x53a, 0x3, 0x2, 0x2, 0x2, 
    0x53e, 0x53f, 0x3, 0x2, 0x2, 0x2, 0x53f, 0x540, 0x3, 0x2, 0x2, 0x2, 
    0x540, 0x545, 0x5, 0xa8, 0x55, 0x2, 0x541, 0x543, 0x7, 0x7c, 0x2, 0x2, 
    0x542, 0x541, 0x3, 0x2, 0x2, 0x2, 0x542, 0x543, 0x3, 0x2, 0x2, 0x2, 
    0x543, 0x544, 0x3, 0x2, 0x2, 0x2, 0x544, 0x546, 0x7, 0x60, 0x2, 0x2, 
    0x545, 0x542, 0x3, 0x2, 0x2, 0x2, 0x545, 0x546, 0x3, 0x2, 0x2, 0x2, 
    0x546, 0xa7, 0x3, 0x2, 0x2, 0x2, 0x547, 0x54b, 0x5, 0xb6, 0x5c, 0x2, 
    0x548, 0x54c, 0x5, 0xb0, 0x59, 0x2, 0x549, 0x54c, 0x5, 0xaa, 0x56, 0x2, 
    0x54a, 0x54c, 0x5, 0xb4, 0x5b, 0x2, 0x54b, 0x548, 0x3, 0x2, 0x2, 0x2, 
    0x54b, 0x549, 0x3, 0x2, 0x2, 0x2, 0x54b, 0x54a, 0x3, 0x2, 0x2, 0x2, 
    0x54b, 0x54c, 0x3, 0x2, 0x2, 0x2, 0x54c, 0xa9, 0x3, 0x2, 0x2, 0x2, 0x54d, 
    0x550, 0x5, 0xac, 0x57, 0x2, 0x54e, 0x550, 0x5, 0xae, 0x58, 0x2, 0x54f, 
    0x54d, 0x3, 0x2, 0x2, 0x2, 0x54f, 0x54e, 0x3, 0x2, 0x2, 0x2, 0x550, 
    0x552, 0x3, 0x2, 0x2, 0x2, 0x551, 0x553, 0x5, 0xaa, 0x56, 0x2, 0x552, 
    0x551, 0x3, 0x2, 0x2, 0x2, 0x552, 0x553, 0x3, 0x2, 0x2, 0x2, 0x553, 
    0xab, 0x3, 0x2, 0x2, 0x2, 0x554, 0x556, 0x7, 0x7c, 0x2, 0x2, 0x555, 
    0x554, 0x3, 0x2, 0x2, 0x2, 0x555, 0x556, 0x3, 0x2, 0x2, 0x2, 0x556, 
    0x557, 0x3, 0x2, 0x2, 0x2, 0x557, 0x558, 0x7, 0x7, 0x2, 0x2, 0x558, 
    0x559, 0x5, 0x86, 0x44, 0x2, 0x559, 0x55a, 0x7, 0x8, 0x2, 0x2, 0x55a, 
    0xad, 0x3, 0x2, 0x2, 0x2, 0x55b, 0x55d, 0x7, 0x7c, 0x2, 0x2, 0x55c, 
    0x55b, 0x3, 0x2, 0x2, 0x2, 0x55c, 0x55d, 0x3, 0x2, 0x2, 0x2, 0x55d, 
    0x55e, 0x3, 0x2, 0x2, 0x2, 0x55e, 0x560, 0x7, 0x7, 0x2, 0x2, 0x55f, 
    0x561, 0x5, 0x86, 0x44, 0x2, 0x560, 0x55f, 0x3, 0x2, 0x2, 0x2, 0x560, 
    0x561, 0x3, 0x2, 0x2, 0x2, 0x561, 0x562, 0x3, 0x2, 0x2, 0x2, 0x562, 
    0x564, 0x7, 0xb, 0x2, 0x2, 0x563, 0x565, 0x5, 0x86, 0x44, 0x2, 0x564, 
    0x563, 0x3, 0x2, 0x2, 0x2, 0x564, 0x565, 0x3, 0x2, 0x2, 0x2, 0x565, 
    0x566, 0x3, 0x2, 0x2, 0x2, 0x566, 0x567, 0x7, 0x8, 0x2, 0x2, 0x567, 
    0xaf, 0x3, 0x2, 0x2, 0x2, 0x568, 0x574, 0x5, 0xb2, 0x5a, 0x2, 0x569, 
    0x56a, 0x7, 0x7c, 0x2, 0x2, 0x56a, 0x56b, 0x7, 0x61, 0x2, 0x2, 0x56b, 
    0x56c, 0x7, 0x7c, 0x2, 0x2, 0x56c, 0x574, 0x7, 0x4b, 0x2, 0x2, 0x56d, 
    0x56e, 0x7, 0x7c, 0x2, 0x2, 0x56e, 0x56f, 0x7, 0x62, 0x2, 0x2, 0x56f, 
    0x570, 0x7, 0x7c, 0x2, 0x2, 0x570, 0x574, 0x7, 0x4b, 0x2, 0x2, 0x571, 
    0x572, 0x7, 0x7c, 0x2, 0x2, 0x572, 0x574, 0x7, 0x63, 0x2, 0x2, 0x573, 
    0x568, 0x3, 0x2, 0x2, 0x2, 0x573, 0x569, 0x3, 0x2, 0x2, 0x2, 0x573, 
    0x56d, 0x3, 0x2, 0x2, 0x2, 0x573, 0x571, 0x3, 0x2, 0x2, 0x2, 0x574, 
    0x576, 0x3, 0x2, 0x2, 0x2, 0x575, 0x577, 0x7, 0x7c, 0x2, 0x2, 0x576, 
    0x575, 0x3, 0x2, 0x2, 0x2, 0x576, 0x577, 0x3, 0x2, 0x2, 0x2, 0x577, 
    0x578, 0x3, 0x2, 0x2, 0x2, 0x578, 0x579, 0x5, 0xb6, 0x5c, 0x2, 0x579, 
    0xb1, 0x3, 0x2, 0x2, 0x2, 0x57a, 0x57c, 0x7, 0x7c, 0x2, 0x2, 0x57b, 
    0x57a, 0x3, 0x2, 0x2, 0x2, 0x57b, 0x57c, 0x3, 0x2, 0x2, 0x2, 0x57c, 
    0x57d, 0x3, 0x2, 0x2, 0x2, 0x57d, 0x57e, 0x7, 0x1b, 0x2, 0x2, 0x57e, 
    0xb3, 0x3, 0x2, 0x2, 0x2, 0x57f, 0x580, 0x7, 0x7c, 0x2, 0x2, 0x580, 
    0x581, 0x7, 0x64, 0x2, 0x2, 0x581, 0x582, 0x7, 0x7c, 0x2, 0x2, 0x582, 
    0x58a, 0x7, 0x65, 0x2, 0x2, 0x583, 0x584, 0x7, 0x7c, 0x2, 0x2, 0x584, 
    0x585, 0x7, 0x64, 0x2, 0x2, 0x585, 0x586, 0x7, 0x7c, 0x2, 0x2, 0x586, 
    0x587, 0x7, 0x5d, 0x2, 0x2, 0x587, 0x588, 0x7, 0x7c, 0x2, 0x2, 0x588, 
    0x58a, 0x7, 0x65, 0x2, 0x2, 0x589, 0x57f, 0x3, 0x2, 0x2, 0x2, 0x589, 
    0x583, 0x3, 0x2, 0x2, 0x2, 0x58a, 0xb5, 0x3, 0x2, 0x2, 0x2, 0x58b, 0x590, 
    0x5, 0xb8, 0x5d, 0x2, 0x58c, 0x58e, 0x7, 0x7c, 0x2, 0x2, 0x58d, 0x58c, 
    0x3, 0x2, 0x2, 0x2, 0x58d, 0x58e, 0x3, 0x2, 0x2, 0x2, 0x58e, 0x58f, 
    0x3, 0x2, 0x2, 0x2, 0x58f, 0x591, 0x5, 0xcc, 0x67, 0x2, 0x590, 0x58d, 
    0x3, 0x2, 0x2, 0x2, 0x590, 0x591, 0x3, 0x2, 0x2, 0x2, 0x591, 0xb7, 0x3, 
    0x2, 0x2, 0x2, 0x592, 0x59a, 0x5, 0xba, 0x5e, 0x2, 0x593, 0x59a, 0x5, 
    0xd6, 0x6c, 0x2, 0x594, 0x59a, 0x5, 0xce, 0x68, 0x2, 0x595, 0x59a, 0x5, 
    0xc4, 0x63, 0x2, 0x596, 0x59a, 0x5, 0xc6, 0x64, 0x2, 0x597, 0x59a, 0x5, 
    0xca, 0x66, 0x2, 0x598, 0x59a, 0x5, 0xd2, 0x6a, 0x2, 0x599, 0x592, 0x3, 
    0x2, 0x2, 0x2, 0x599, 0x593, 0x3, 0x2, 0x2, 0x2, 0x599, 0x594, 0x3, 
    0x2, 0x2, 0x2, 0x599, 0x595, 0x3, 0x2, 0x2, 0x2, 0x599, 0x596, 0x3, 
    0x2, 0x2, 0x2, 0x599, 0x597, 0x3, 0x2, 0x2, 0x2, 0x599, 0x598, 0x3, 
    0x2, 0x2, 0x2, 0x59a, 0xb9, 0x3, 0x2, 0x2, 0x2, 0x59b, 0x5a2, 0x5, 0xd4, 
    0x6b, 0x2, 0x59c, 0x5a2, 0x7, 0x6e, 0x2, 0x2, 0x59d, 0x5a2, 0x5, 0xbc, 
    0x5f, 0x2, 0x59e, 0x5a2, 0x7, 0x65, 0x2, 0x2, 0x59f, 0x5a2, 0x5, 0xbe, 
    0x60, 0x2, 0x5a0, 0x5a2, 0x5, 0xc0, 0x61, 0x2, 0x5a1, 0x59b, 0x3, 0x2, 
    0x2, 0x2, 0x5a1, 0x59c, 0x3, 0x2, 0x2, 0x2, 0x5a1, 0x59d, 0x3, 0x2, 
    0x2, 0x2, 0x5a1, 0x59e, 0x3, 0x2, 0x2, 0x2, 0x5a1, 0x59f, 0x3, 0x2, 
    0x2, 0x2, 0x5a1, 0x5a0, 0x3, 0x2, 0x2, 0x2, 0x5a2, 0xbb, 0x3, 0x2, 0x2, 
    0x2, 0x5a3, 0x5a4, 0x9, 0x7, 0x2, 0x2, 0x5a4, 0xbd, 0x3, 0x2, 0x2, 0x2, 
    0x5a5, 0x5a7, 0x7, 0x7, 0x2, 0x2, 0x5a6, 0x5a8, 0x7, 0x7c, 0x2, 0x2, 
    0x5a7, 0x5a6, 0x3, 0x2, 0x2, 0x2, 0x5a7, 0x5a8, 0x3, 0x2, 0x2, 0x2, 
    0x5a8, 0x5ba, 0x3, 0x2, 0x2, 0x2, 0x5a9, 0x5ab, 0x5, 0x86, 0x44, 0x2, 
    0x5aa, 0x5ac, 0x7, 0x7c, 0x2, 0x2, 0x5ab, 0x5aa, 0x3, 0x2, 0x2, 0x2, 
    0x5ab, 0x5ac, 0x3, 0x2, 0x2, 0x2, 0x5ac, 0x5b7, 0x3, 0x2, 0x2, 0x2, 
    0x5ad, 0x5af, 0x7, 0x6, 0x2, 0x2, 0x5ae, 0x5b0, 0x7, 0x7c, 0x2, 0x2, 
    0x5af, 0x5ae, 0x3, 0x2, 0x2, 0x2, 0x5af, 0x5b0, 0x3, 0x2, 0x2, 0x2, 
    0x5b0, 0x5b1, 0x3, 0x2, 0x2, 0x2, 0x5b1, 0x5b3, 0x5, 0x86, 0x44, 0x2, 
    0x5b2, 0x5b4, 0x7, 0x7c, 0x2, 0x2, 0x5b3, 0x5b2, 0x3, 0x2, 0x2, 0x2, 
    0x5b3, 0x5b4, 0x3, 0x2, 0x2, 0x2, 0x5b4, 0x5b6, 0x3, 0x2, 0x2, 0x2, 
    0x5b5, 0x5ad, 0x3, 0x2, 0x2, 0x2, 0x5b6, 0x5b9, 0x3, 0x2, 0x2, 0x2, 
    0x5b7, 0x5b5, 0x3, 0x2, 0x2, 0x2, 0x5b7, 0x5b8, 0x3, 0x2, 0x2, 0x2, 
    0x5b8, 0x5bb, 0x3, 0x2, 0x2, 0x2, 0x5b9, 0x5b7, 0x3, 0x2, 0x2, 0x2, 
    0x5ba, 0x5a9, 0x3, 0x2, 0x2, 0x2, 0x5ba, 0x5bb, 0x3, 0x2, 0x2, 0x2, 
    0x5bb, 0x5bc, 0x3, 0x2, 0x2, 0x2, 0x5bc, 0x5bd, 0x7, 0x8, 0x2, 0x2, 
    0x5bd, 0xbf, 0x3, 0x2, 0x2, 0x2, 0x5be, 0x5c0, 0x7, 0xa, 0x2, 0x2, 0x5bf, 
    0x5c1, 0x7, 0x7c, 0x2, 0x2, 0x5c0, 0x5bf, 0x3, 0x2, 0x2, 0x2, 0x5c0, 
    0x5c1, 0x3, 0x2, 0x2, 0x2, 0x5c1, 0x5c2, 0x3, 0x2, 0x2, 0x2, 0x5c2, 
    0x5c4, 0x5, 0xc2, 0x62, 0x2, 0x5c3, 0x5c5, 0x7, 0x7c, 0x2, 0x2, 0x5c4, 
    0x5c3, 0x3, 0x2, 0x2, 0x2, 0x5c4, 0x5c5, 0x3, 0x2, 0x2, 0x2, 0x5c5, 
    0x5d0, 0x3, 0x2, 0x2, 0x2, 0x5c6, 0x5c8, 0x7, 0x6, 0x2, 0x2, 0x5c7, 
    0x5c9, 0x7, 0x7c, 0x2, 0x2, 0x5c8, 0x5c7, 0x3, 0x2, 0x2, 0x2, 0x5c8, 
    0x5c9, 0x3, 0x2, 0x2, 0x2, 0x5c9, 0x5ca, 0x3, 0x2, 0x2, 0x2, 0x5ca, 
    0x5cc, 0x5, 0xc2, 0x62, 0x2, 0x5cb, 0x5cd, 0x7, 0x7c, 0x2, 0x2, 0x5cc, 
    0x5cb, 0x3, 0x2, 0x2, 0x2, 0x5cc, 0x5cd, 0x3, 0x2, 0x2, 0x2, 0x5cd, 
    0x5cf, 0x3, 0x2, 0x2, 0x2, 0x5ce, 0x5c6, 0x3, 0x2, 0x2, 0x2, 0x5cf, 
    0x5d2, 0x3, 0x2, 0x2, 0x2, 0x5d0, 0x5ce, 0x3, 0x2, 0x2, 0x2, 0x5d0, 
    0x5d1, 0x3, 0x2, 0x2, 0x2, 0x5d1, 0x5d3, 0x3, 0x2, 0x2, 0x2, 0x5d2, 
    0x5d0, 0x3, 0x2, 0x2, 0x2, 0x5d3, 0x5d4, 0x7, 0xc, 0x2, 0x2, 0x5d4, 
    0xc1, 0x3, 0x2, 0x2, 0x2, 0x5d5, 0x5d7, 0x5, 0xe2, 0x72, 0x2, 0x5d6, 
    0x5d8, 0x7, 0x7c, 0x2, 0x2, 0x5d7, 0x5d6, 0x3, 0x2, 0x2, 0x2, 0x5d7, 
    0x5d8, 0x3, 0x2, 0x2, 0x2, 0x5d8, 0x5d9, 0x3, 0x2, 0x2, 0x2, 0x5d9, 
    0x5db, 0x7, 0xb, 0x2, 0x2, 0x5da, 0x5dc, 0x7, 0x7c, 0x2, 0x2, 0x5db, 
    0x5da, 0x3, 0x2, 0x2, 0x2, 0x5db, 0x5dc, 0x3, 0x2, 0x2, 0x2, 0x5dc, 
    0x5dd, 0x3, 0x2, 0x2, 0x2, 0x5dd, 0x5de, 0x5, 0x86, 0x44, 0x2, 0x5de, 
    0xc3, 0x3, 0x2, 0x2, 0x2, 0x5df, 0x5e1, 0x7, 0x4, 0x2, 0x2, 0x5e0, 0x5e2, 
    0x7, 0x7c, 0x2, 0x2, 0x5e1, 0x5e0, 0x3, 0x2, 0x2, 0x2, 0x5e1, 0x5e2, 
    0x3, 0x2, 0x2, 0x2, 0x5e2, 0x5e3, 0x3, 0x2, 0x2, 0x2, 0x5e3, 0x5e5, 
    0x5, 0x86, 0x44, 0x2, 0x5e4, 0x5e6, 0x7, 0x7c, 0x2, 0x2, 0x5e5, 0x5e4, 
    0x3, 0x2, 0x2, 0x2, 0x5e5, 0x5e6, 0x3, 0x2, 0x2, 0x2, 0x5e6, 0x5e7, 
    0x3, 0x2, 0x2, 0x2, 0x5e7, 0x5e8, 0x7, 0x5, 0x2, 0x2, 0x5e8, 0xc5, 0x3, 
    0x2, 0x2, 0x2, 0x5e9, 0x5eb, 0x5, 0xc8, 0x65, 0x2, 0x5ea, 0x5ec, 0x7, 
    0x7c, 0x2, 0x2, 0x5eb, 0x5ea, 0x3, 0x2, 0x2, 0x2, 0x5eb, 0x5ec, 0x3, 
    0x2, 0x2, 0x2, 0x5ec, 0x5ed, 0x3, 0x2, 0x2, 0x2, 0x5ed, 0x5ef, 0x7, 
    0x4, 0x2, 0x2, 0x5ee, 0x5f0, 0x7, 0x7c, 0x2, 0x2, 0x5ef, 0x5ee, 0x3, 
    0x2, 0x2, 0x2, 0x5ef, 0x5f0, 0x3, 0x2, 0x2, 0x2, 0x5f0, 0x5f1, 0x3, 
    0x2, 0x2, 0x2, 0x5f1, 0x5f3, 0x7, 0x4e, 0x2, 0x2, 0x5f2, 0x5f4, 0x7, 
    0x7c, 0x2, 0x2, 0x5f3, 0x5f2, 0x3, 0x2, 0x2, 0x2, 0x5f3, 0x5f4, 0x3, 
    0x2, 0x2, 0x2, 0x5f4, 0x5f5, 0x3, 0x2, 0x2, 0x2, 0x5f5, 0x5f6, 0x7, 
    0x5, 0x2, 0x2, 0x5f6, 0x61b, 0x3, 0x2, 0x2, 0x2, 0x5f7, 0x5f9, 0x5, 
    0xc8, 0x65, 0x2, 0x5f8, 0x5fa, 0x7, 0x7c, 0x2, 0x2, 0x5f9, 0x5f8, 0x3, 
    0x2, 0x2, 0x2, 0x5f9, 0x5fa, 0x3, 0x2, 0x2, 0x2, 0x5fa, 0x5fb, 0x3, 
    0x2, 0x2, 0x2, 0x5fb, 0x5fd, 0x7, 0x4, 0x2, 0x2, 0x5fc, 0x5fe, 0x7, 
    0x7c, 0x2, 0x2, 0x5fd, 0x5fc, 0x3, 0x2, 0x2, 0x2, 0x5fd, 0x5fe, 0x3, 
    0x2, 0x2, 0x2, 0x5fe, 0x603, 0x3, 0x2, 0x2, 0x2, 0x5ff, 0x601, 0x7, 
    0x4d, 0x2, 0x2, 0x600, 0x602, 0x7, 0x7c, 0x2, 0x2, 0x601, 0x600, 0x3, 
    0x2, 0x2, 0x2, 0x601, 0x602, 0x3, 0x2, 0x2, 0x2, 0x602, 0x604, 0x3, 
    0x2, 0x2, 0x2, 0x603, 0x5ff, 0x3, 0x2, 0x2, 0x2, 0x603, 0x604, 0x3, 
    0x2, 0x2, 0x2, 0x604, 0x616, 0x3, 0x2, 0x2, 0x2, 0x605, 0x607, 0x5, 
    0x86, 0x44, 0x2, 0x606, 0x608, 0x7, 0x7c, 0x2, 0x2, 0x607, 0x606, 0x3, 
    0x2, 0x2, 0x2, 0x607, 0x608, 0x3, 0x2, 0x2, 0x2, 0x608, 0x613, 0x3, 
    0x2, 0x2, 0x2, 0x609, 0x60b, 0x7, 0x6, 0x2, 0x2, 0x60a, 0x60c, 0x7, 
    0x7c, 0x2, 0x2, 0x60b, 0x60a, 0x3, 0x2, 0x2, 0x2, 0x60b, 0x60c, 0x3, 
    0x2, 0x2, 0x2, 0x60c, 0x60d, 0x3, 0x2, 0x2, 0x2, 0x60d, 0x60f, 0x5, 
    0x86, 0x44, 0x2, 0x60e, 0x610, 0x7, 0x7c, 0x2, 0x2, 0x60f, 0x60e, 0x3, 
    0x2, 0x2, 0x2, 0x60f, 0x610, 0x3, 0x2, 0x2, 0x2, 0x610, 0x612, 0x3, 
    0x2, 0x2, 0x2, 0x611, 0x609, 0x3, 0x2, 0x2, 0x2, 0x612, 0x615, 0x3, 
    0x2, 0x2, 0x2, 0x613, 0x611, 0x3, 0x2, 0x2, 0x2, 0x613, 0x614, 0x3, 
    0x2, 0x2, 0x2, 0x614, 0x617, 0x3, 0x2, 0x2, 0x2, 0x615, 0x613, 0x3, 
    0x2, 0x2, 0x2, 0x616, 0x605, 0x3, 0x2, 0x2, 0x2, 0x616, 0x617, 0x3, 
    0x2, 0x2, 0x2, 0x617, 0x618, 0x3, 0x2, 0x2, 0x2, 0x618, 0x619, 0x7, 
    0x5, 0x2, 0x2, 0x619, 0x61b, 0x3, 0x2, 0x2, 0x2, 0x61a, 0x5e9, 0x3, 
    0x2, 0x2, 0x2, 0x61a, 0x5f7, 0x3, 0x2, 0x2, 0x2, 0x61b, 0xc7, 0x3, 0x2, 
    0x2, 0x2, 0x61c, 0x61d, 0x5, 0xe2, 0x72, 0x2, 0x61d, 0xc9, 0x3, 0x2, 
    0x2, 0x2, 0x61e, 0x620, 0x7, 0x68, 0x2, 0x2, 0x61f, 0x621, 0x7, 0x7c, 
    0x2, 0x2, 0x620, 0x61f, 0x3, 0x2, 0x2, 0x2, 0x620, 0x621, 0x3, 0x2, 
    0x2, 0x2, 0x621, 0x622, 0x3, 0x2, 0x2, 0x2, 0x622, 0x624, 0x7, 0xa, 
    0x2, 0x2, 0x623, 0x625, 0x7, 0x7c, 0x2, 0x2, 0x624, 0x623, 0x3, 0x2, 
    0x2, 0x2, 0x624, 0x625, 0x3, 0x2, 0x2, 0x2, 0x625, 0x626, 0x3, 0x2, 
    0x2, 0x2, 0x626, 0x628, 0x7, 0x46, 0x2, 0x2, 0x627, 0x629, 0x7, 0x7c, 
    0x2, 0x2, 0x628, 0x627, 0x3, 0x2, 0x2, 0x2, 0x628, 0x629, 0x3, 0x2, 
    0x2, 0x2, 0x629, 0x62a, 0x3, 0x2, 0x2, 0x2, 0x62a, 0x62f, 0x5, 0x68, 
    0x35, 0x2, 0x62b, 0x62d, 0x7, 0x7c, 0x2, 0x2, 0x62c, 0x62b, 0x3, 0x2, 
    0x2, 0x2, 0x62c, 0x62d, 0x3, 0x2, 0x2, 0x2, 0x62d, 0x62e, 0x3, 0x2, 
    0x2, 0x2, 0x62e, 0x630, 0x5, 0x66, 0x34, 0x2, 0x62f, 0x62c, 0x3, 0x2, 
    0x2, 0x2, 0x62f, 0x630, 0x3, 0x2, 0x2, 0x2, 0x630, 0x632, 0x3, 0x2, 
    0x2, 0x2, 0x631, 0x633, 0x7, 0x7c, 0x2, 0x2, 0x632, 0x631, 0x3, 0x2, 
    0x2, 0x2, 0x632, 0x633, 0x3, 0x2, 0x2, 0x2, 0x633, 0x634, 0x3, 0x2, 
    0x2, 0x2, 0x634, 0x635, 0x7, 0xc, 0x2, 0x2, 0x635, 0xcb, 0x3, 0x2, 0x2, 
    0x2, 0x636, 0x638, 0x7, 0x1c, 0x2, 0x2, 0x637, 0x639, 0x7, 0x7c, 0x2, 
    0x2, 0x638, 0x637, 0x3, 0x2, 0x2, 0x2, 0x638, 0x639, 0x3, 0x2, 0x2, 
    0x2, 0x639, 0x63a, 0x3, 0x2, 0x2, 0x2, 0x63a, 0x63b, 0x5, 0xda, 0x6e, 
    0x2, 0x63b, 0xcd, 0x3, 0x2, 0x2, 0x2, 0x63c, 0x641, 0x7, 0x69, 0x2, 
    0x2, 0x63d, 0x63f, 0x7, 0x7c, 0x2, 0x2, 0x63e, 0x63d, 0x3, 0x2, 0x2, 
    0x2, 0x63e, 0x63f, 0x3, 0x2, 0x2, 0x2, 0x63f, 0x640, 0x3, 0x2, 0x2, 
    0x2, 0x640, 0x642, 0x5, 0xd0, 0x69, 0x2, 0x641, 0x63e, 0x3, 0x2, 0x2, 
    0x2, 0x642, 0x643, 0x3, 0x2, 0x2, 0x2, 0x643, 0x641, 0x3, 0x2, 0x2, 
    0x2, 0x643, 0x644, 0x3, 0x2, 0x2, 0x2, 0x644, 0x653, 0x3, 0x2, 0x2, 
    0x2, 0x645, 0x647, 0x7, 0x69, 0x2, 0x2, 0x646, 0x648, 0x7, 0x7c, 0x2, 
    0x2, 0x647, 0x646, 0x3, 0x2, 0x2, 0x2, 0x647, 0x648, 0x3, 0x2, 0x2, 
    0x2, 0x648, 0x649, 0x3, 0x2, 0x2, 0x2, 0x649, 0x64e, 0x5, 0x86, 0x44, 
    0x2, 0x64a, 0x64c, 0x7, 0x7c, 0x2, 0x2, 0x64b, 0x64a, 0x3, 0x2, 0x2, 
    0x2, 0x64b, 0x64c, 0x3, 0x2, 0x2, 0x2, 0x64c, 0x64d, 0x3, 0x2, 0x2, 
    0x2, 0x64d, 0x64f, 0x5, 0xd0, 0x69, 0x2, 0x64e, 0x64b, 0x3, 0x2, 0x2, 
    0x2, 0x64f, 0x650, 0x3, 0x2, 0x2, 0x2, 0x650, 0x64e, 0x3, 0x2, 0x2, 
    0x2, 0x650, 0x651, 0x3, 0x2, 0x2, 0x2, 0x651, 0x653, 0x3, 0x2, 0x2, 
    0x2, 0x652, 0x63c, 0x3, 0x2, 0x2, 0x2, 0x652, 0x645, 0x3, 0x2, 0x2, 
    0x2, 0x653, 0x65c, 0x3, 0x2, 0x2, 0x2, 0x654, 0x656, 0x7, 0x7c, 0x2, 
    0x2, 0x655, 0x654, 0x3, 0x2, 0x2, 0x2, 0x655, 0x656, 0x3, 0x2, 0x2, 
    0x2, 0x656, 0x657, 0x3, 0x2, 0x2, 0x2, 0x657, 0x659, 0x7, 0x6a, 0x2, 
    0x2, 0x658, 0x65a, 0x7, 0x7c, 0x2, 0x2, 0x659, 0x658, 0x3, 0x2, 0x2, 
    0x2, 0x659, 0x65a, 0x3, 0x2, 0x2, 0x2, 0x65a, 0x65b, 0x3, 0x2, 0x2, 
    0x2, 0x65b, 0x65d, 0x5, 0x86, 0x44, 0x2, 0x65c, 0x655, 0x3, 0x2, 0x2, 
    0x2, 0x65c, 0x65d, 0x3, 0x2, 0x2, 0x2, 0x65d, 0x65f, 0x3, 0x2, 0x2, 
    0x2, 0x65e, 0x660, 0x7, 0x7c, 0x2, 0x2, 0x65f, 0x65e, 0x3, 0x2, 0x2, 
    0x2, 0x65f, 0x660, 0x3, 0x2, 0x2, 0x2, 0x660, 0x661, 0x3, 0x2, 0x2, 
    0x2, 0x661, 0x662, 0x7, 0x6b, 0x2, 0x2, 0x662, 0xcf, 0x3, 0x2, 0x2, 
    0x2, 0x663, 0x665, 0x7, 0x6c, 0x2, 0x2, 0x664, 0x666, 0x7, 0x7c, 0x2, 
    0x2, 0x665, 0x664, 0x3, 0x2, 0x2, 0x2, 0x665, 0x666, 0x3, 0x2, 0x2, 
    0x2, 0x666, 0x667, 0x3, 0x2, 0x2, 0x2, 0x667, 0x669, 0x5, 0x86, 0x44, 
    0x2, 0x668, 0x66a, 0x7, 0x7c, 0x2, 0x2, 0x669, 0x668, 0x3, 0x2, 0x2, 
    0x2, 0x669, 0x66a, 0x3, 0x2, 0x2, 0x2, 0x66a, 0x66b, 0x3, 0x2, 0x2, 
    0x2, 0x66b, 0x66d, 0x7, 0x6d, 0x2, 0x2, 0x66c, 0x66e, 0x7, 0x7c, 0x2, 
    0x2, 0x66d, 0x66c, 0x3, 0x2, 0x2, 0x2, 0x66d, 0x66e, 0x3, 0x2, 0x2, 
    0x2, 0x66e, 0x66f, 0x3, 0x2, 0x2, 0x2, 0x66f, 0x670, 0x5, 0x86, 0x44, 
    0x2, 0x670, 0xd1, 0x3, 0x2, 0x2, 0x2, 0x671, 0x672, 0x5, 0xe2, 0x72, 
    0x2, 0x672, 0xd3, 0x3, 0x2, 0x2, 0x2, 0x673, 0x676, 0x5, 0xde, 0x70, 
    0x2, 0x674, 0x676, 0x5, 0xdc, 0x6f, 0x2, 0x675, 0x673, 0x3, 0x2, 0x2, 
    0x2, 0x675, 0x674, 0x3, 0x2, 0x2, 0x2, 0x676, 0xd5, 0x3, 0x2, 0x2, 0x2, 
    0x677, 0x67a, 0x7, 0x1d, 0x2, 0x2, 0x678, 0x67b, 0x5, 0xe2, 0x72, 0x2, 
    0x679, 0x67b, 0x7, 0x70, 0x2, 0x2, 0x67a, 0x678, 0x3, 0x2, 0x2, 0x2, 
    0x67a, 0x679, 0x3, 0x2, 0x2, 0x2, 0x67b, 0xd7, 0x3, 0x2, 0x2, 0x2, 0x67c, 
    0x67e, 0x5, 0xb8, 0x5d, 0x2, 0x67d, 0x67f, 0x7, 0x7c, 0x2, 0x2, 0x67e, 
    0x67d, 0x3, 0x2, 0x2, 0x2, 0x67e, 0x67f, 0x3, 0x2, 0x2, 0x2, 0x67f, 
    0x680, 0x3, 0x2, 0x2, 0x2, 0x680, 0x681, 0x5, 0xcc, 0x67, 0x2, 0x681, 
    0xd9, 0x3, 0x2, 0x2, 0x2, 0x682, 0x683, 0x5, 0xe0, 0x71, 0x2, 0x683, 
    0xdb, 0x3, 0x2, 0x2, 0x2, 0x684, 0x685, 0x7, 0x70, 0x2, 0x2, 0x685, 
    0xdd, 0x3, 0x2, 0x2, 0x2, 0x686, 0x687, 0x7, 0x77, 0x2, 0x2, 0x687, 
    0xdf, 0x3, 0x2, 0x2, 0x2, 0x688, 0x689, 0x5, 0xe2, 0x72, 0x2, 0x689, 
    0xe1, 0x3, 0x2, 0x2, 0x2, 0x68a, 0x68f, 0x7, 0x78, 0x2, 0x2, 0x68b, 
    0x68c, 0x7, 0x7b, 0x2, 0x2, 0x68c, 0x68f, 0x8, 0x72, 0x1, 0x2, 0x68d, 
    0x68f, 0x7, 0x71, 0x2, 0x2, 0x68e, 0x68a, 0x3, 0x2, 0x2, 0x2, 0x68e, 
    0x68b, 0x3, 0x2, 0x2, 0x2, 0x68e, 0x68d, 0x3, 0x2, 0x2, 0x2, 0x68f, 
    0xe3, 0x3, 0x2, 0x2, 0x2, 0x690, 0x691, 0x9, 0x8, 0x2, 0x2, 0x691, 0xe5, 
    0x3, 0x2, 0x2, 0x2, 0x692, 0x693, 0x9, 0x9, 0x2, 0x2, 0x693, 0xe7, 0x3, 
    0x2, 0x2, 0x2, 0x694, 0x695, 0x9, 0xa, 0x2, 0x2, 0x695, 0xe9, 0x3, 0x2, 
    0x2, 0x2, 0x126, 0xeb, 0xee, 0xf1, 0xf7, 0xfa, 0xfd, 0x100, 0x10c, 0x110, 
    0x114, 0x118, 0x122, 0x126, 0x12a, 0x12f, 0x13a, 0x13e, 0x142, 0x147, 
    0x14e, 0x152, 0x156, 0x159, 0x15d, 0x161, 0x166, 0x16b, 0x16f, 0x177, 
    0x181, 0x185, 0x189, 0x18d, 0x192, 0x19e, 0x1a2, 0x1ac, 0x1b0, 0x1b4, 
    0x1b6, 0x1ba, 0x1be, 0x1c0, 0x1d6, 0x1e1, 0x1f7, 0x1fb, 0x200, 0x20b, 
    0x20f, 0x213, 0x21d, 0x221, 0x225, 0x229, 0x22f, 0x234, 0x23a, 0x246, 
    0x24b, 0x250, 0x254, 0x259, 0x25f, 0x264, 0x267, 0x26b, 0x26f, 0x273, 
    0x279, 0x27d, 0x282, 0x287, 0x28b, 0x28e, 0x292, 0x296, 0x29a, 0x29e, 
    0x2a2, 0x2a8, 0x2ac, 0x2b1, 0x2b5, 0x2bd, 0x2c1, 0x2c5, 0x2c9, 0x2cd, 
    0x2d0, 0x2d4, 0x2de, 0x2e4, 0x2e8, 0x2ec, 0x2f1, 0x2f6, 0x2fa, 0x300, 
    0x304, 0x308, 0x30d, 0x313, 0x316, 0x31c, 0x31f, 0x325, 0x329, 0x32d, 
    0x331, 0x335, 0x33a, 0x33f, 0x343, 0x348, 0x34b, 0x354, 0x35d, 0x362, 
    0x36f, 0x372, 0x37a, 0x37e, 0x383, 0x38c, 0x391, 0x398, 0x39c, 0x3a0, 
    0x3a2, 0x3a6, 0x3a8, 0x3ac, 0x3ae, 0x3b2, 0x3b6, 0x3b8, 0x3bc, 0x3be, 
    0x3c2, 0x3c4, 0x3c7, 0x3cb, 0x3d1, 0x3d5, 0x3d8, 0x3db, 0x3e1, 0x3e4, 
    0x3e7, 0x3eb, 0x3f1, 0x3f4, 0x3f7, 0x3fb, 0x3ff, 0x403, 0x405, 0x409, 
    0x40b, 0x40f, 0x411, 0x415, 0x417, 0x41d, 0x421, 0x425, 0x429, 0x42d, 
    0x431, 0x435, 0x439, 0x43d, 0x440, 0x446, 0x44a, 0x44e, 0x451, 0x456, 
    0x45b, 0x460, 0x465, 0x46b, 0x46e, 0x471, 0x475, 0x479, 0x48a, 0x494, 
    0x49e, 0x4a3, 0x4a5, 0x4ab, 0x4af, 0x4b3, 0x4b7, 0x4bb, 0x4c3, 0x4c7, 
    0x4cb, 0x4cf, 0x4d5, 0x4d9, 0x4df, 0x4e3, 0x4e8, 0x4ed, 0x4f1, 0x4f6, 
    0x4fb, 0x4ff, 0x505, 0x50c, 0x510, 0x516, 0x51d, 0x521, 0x527, 0x52e, 
    0x532, 0x537, 0x53c, 0x53e, 0x542, 0x545, 0x54b, 0x54f, 0x552, 0x555, 
    0x55c, 0x560, 0x564, 0x573, 0x576, 0x57b, 0x589, 0x58d, 0x590, 0x599, 
    0x5a1, 0x5a7, 0x5ab, 0x5af, 0x5b3, 0x5b7, 0x5ba, 0x5c0, 0x5c4, 0x5c8, 
    0x5cc, 0x5d0, 0x5d7, 0x5db, 0x5e1, 0x5e5, 0x5eb, 0x5ef, 0x5f3, 0x5f9, 
    0x5fd, 0x601, 0x603, 0x607, 0x60b, 0x60f, 0x613, 0x616, 0x61a, 0x620, 
    0x624, 0x628, 0x62c, 0x62f, 0x632, 0x638, 0x63e, 0x643, 0x647, 0x64b, 
    0x650, 0x652, 0x655, 0x659, 0x65c, 0x65f, 0x665, 0x669, 0x66d, 0x675, 
    0x67a, 0x67e, 0x68e, 
  };

  atn::ATNDeserializer deserializer;
  _atn = deserializer.deserialize(_serializedATN);

  size_t count = _atn.getNumberOfDecisions();
  _decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) { 
    _decisionToDFA.emplace_back(_atn.getDecisionState(i), i);
  }
}

CypherParser::Initializer CypherParser::_init;

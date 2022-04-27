REBAR = $(shell command -v rebar3 || echo ./rebar3)

.PHONY: all compile test qc clean

all: compile

compile:
	@$(REBAR) compile

test:
	@$(REBAR) eunit
	@$(REBAR) proper -n 1

qc: compile
	@$(REBAR) eqc

clean:
	@$(REBAR) clean

dialyze:
	@$(REBAR) dialyzer

edoc:
	mkdir doc && cp -fR doc_src/* doc
	$(REBAR) edoc
	
edoc_private:
	mkdir doc && cp -fR doc_src/* doc
	$(REBAR) as edoc_private edoc
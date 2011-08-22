.PHONY: all compile clean test

all: compile

compile:
	@./rebar compile

clean:
	@./rebar clean

test:
	@./rebar compile eunit

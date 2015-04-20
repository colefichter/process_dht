#!/bin/sh
#erl -pa ebin -s eunit test -s init stop

erl -noshell -pa ebin -eval 'eunit:test("ebin",[verbose])' -s init stop
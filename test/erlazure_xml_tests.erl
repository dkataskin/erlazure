%% Copyright (c) 2013 - 2014, Dmitry Kataskin
%% All rights reserved.
%%
%% Redistribution and use in source and binary forms, with or without
%% modification, are permitted provided that the following conditions are met:
%%
%% * Redistributions of source code must retain the above copyright notice,
%% this list of conditions and the following disclaimer.
%% * Redistributions in binary form must reproduce the above copyright
%% notice, this list of conditions and the following disclaimer in the
%% documentation and/or other materials provided with the distribution.
%% * Neither the name of erlazure nor the names of its contributors may be used to
%% endorse or promote products derived from this software without specific
%% prior written permission.
%%
%% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
%% AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
%% IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
%% ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
%% LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
%% CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
%% SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
%% INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
%% CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
%% ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
%% POSSIBILITY OF SUCH DAMAGE.

-module(erlazure_xml_tests).
-author("Dmitry Kataskin").

-compile(export_all).

-include("erlazure.hrl").
-include_lib("eunit/include/eunit.hrl").

parse_enumeration_test() ->
                Response = test_utils:read_file("enumeration_result.xml"),
                {ParseResult, _} = xmerl_scan:string(Response),

                ParserSpec = #enum_parser_spec { rootKey = 'Items',
                                                 elementKey = 'Item',
                                                 elementParser = fun parse_test_item/1 },
                {ok, Result} = erlazure_xml:parse_enumeration(ParseResult, ParserSpec),
                ?assertMatch({[[{id, 1}, {property1, "Value1"}],
                               [{id, 2}, {property1, "Value2"}]],
                              [{prefix, "prfx"},
                               {marker, "mrkr"},
                               {max_results, 154},
                               {delimiter, "dlmtr"},
                               {next_marker, "ee948889-5a03-46dd-843a-22a7f12d7124"}]}, Result).

parse_test_item(#xmlElement{ content = Content }) ->
                Nodes = erlazure_xml:filter_elements(Content),
                FoldFun = fun(Elem=#xmlElement{}, Acc) ->
                  case Elem#xmlElement.name of
                    'Id' -> [{id, erlazure_xml:parse_int(Elem)} | Acc];
                    'Property1' -> [{property1, erlazure_xml:parse_str(Elem)} | Acc];
                    _ -> Acc
                  end
                end,
                lists:reverse(lists:foldl(FoldFun, [], Nodes)).
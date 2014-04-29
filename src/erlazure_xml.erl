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
%% * Neither the name of  nor the names of its contributors may be used to
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

-module(erlazure_xml).
-author("Dmitry Kataskin").

-include("erlazure.hrl").
-include_lib("xmerl/include/xmerl.hrl").

%% API
-export([get_element_text/2, parse_metadata/1, parse_list/2, parse_enumeration/2, filter_elements/1, get_text/1,
         parse_str/1, parse_int/1]).

get_element_text(ElementName, Elements) when is_list(ElementName), is_list(Elements) ->
            case lists:keyfind(ElementName, 1, Elements) of
              {ElementName, _, Value} -> lists:flatten(Value);
              false -> ""
            end.

parse_list(ParseFun, List) ->
            FoldFun = fun(Element, Acc) ->
              [ParseFun(Element) | Acc]
            end,
            lists:reverse(lists:foldl(FoldFun, [], List)).

parse_metadata(#xmlElement { content = Content }) ->
            Nodes = erlazure_xml:filter_elements(Content),
            lists:foldl(fun parse_metadata/2, [], Nodes).

parse_metadata(Elem=#xmlElement{}, Items) ->
            [{Elem#xmlElement.name, erlazure_xml:parse_str(Elem)} | Items].

parse_common_tokens(Elem=#xmlElement{}, Tokens) ->
            case Elem#xmlElement.name of
              'Prefix' -> [{prefix, erlazure_xml:parse_str(Elem)} | Tokens];
              'Marker' -> [{marker, erlazure_xml:parse_str(Elem)} | Tokens];
              'MaxResults' -> [{max_results, erlazure_xml:parse_int(Elem)} | Tokens];
              'NextMarker' -> [{next_marker, erlazure_xml:parse_str(Elem)} | Tokens];
              _ -> Tokens
            end.

parse_enumeration(Elem=#xmlElement{}, ParseFun) ->
            case Elem#xmlElement.name of
              'EnumerationResults' ->
                Nodes = erlazure_xml:filter_elements(Elem#xmlElement.content),
                CommonTokens = lists:foldl(fun parse_common_tokens/2, [], Nodes),
                Items = lists:foldl(ParseFun, [], Nodes),
                {ok, {lists:reverse(Items), lists:reverse(CommonTokens)}};

              _ -> {error, bad_response}
            end.

filter_elements(XmlNodes) ->
            lists:filter(fun(Elem) when is_record(Elem, xmlElement) -> true;
                            (_) -> false end, XmlNodes).

get_text(#xmlElement { content = Content }) ->
            case Content of
              [{xmlText, _, _, _, Text, _}] -> Text;
              _ -> ""
            end.

parse_str(XmlElement=#xmlElement{}) ->
            get_text(XmlElement).

parse_int(XmlElement=#xmlElement{}) ->
            erlang:list_to_integer(get_text(XmlElement)).
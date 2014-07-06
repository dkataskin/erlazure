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

-module(erlazure_xml).
-author("Dmitry Kataskin").

-include("erlazure.hrl").

-record(parse_enum_acc, { items=[], misc=[], custom=[], spec = #enum_parser_spec{} }).

%% API
-export([parse_metadata/1, parse_list/2, parse_enumeration/2, parse_str/1, parse_int/1]).
-export([get_element_text/2, filter_elements/1, get_text/1]).

get_element_text(ElementName, Elements) when is_list(ElementName), is_list(Elements) ->
        case lists:keyfind(ElementName, 1, Elements) of
          {ElementName, _, Value} -> lists:flatten(Value);
          false -> ""
        end.

parse_metadata(#xmlElement { content = Content }) ->
        Nodes = erlazure_xml:filter_elements(Content),
        FoldFun = fun(MetadataElem=#xmlElement{}, Metadata) ->
                    [{MetadataElem#xmlElement.name, erlazure_xml:parse_str(MetadataElem)} | Metadata]
                  end,
        lists:reverse(lists:foldl(FoldFun, [], Nodes)).

parse_common_tokens(Elem=#xmlElement{}, Tokens) ->
        case Elem#xmlElement.name of
          'Prefix' -> [{prefix, erlazure_xml:parse_str(Elem)} | Tokens];
          'Marker' -> [{marker, erlazure_xml:parse_str(Elem)} | Tokens];
          'MaxResults' -> [{max_results, erlazure_xml:parse_int(Elem)} | Tokens];
          'NextMarker' -> [{next_marker, erlazure_xml:parse_str(Elem)} | Tokens];
          'Delimiter' -> [{delimiter, erlazure_xml:parse_str(Elem)} | Tokens];
          _ -> Tokens
        end.

parse_enumeration(Response, ParserSpec=#enum_parser_spec{}) when is_binary(Response) ->
        parse_enumeration(erlang:binary_to_list(Response), ParserSpec);

parse_enumeration(Response, ParserSpec=#enum_parser_spec{}) when is_list(Response) ->
        {ParseResult, _} = xmerl_scan:string(Response),
        erlazure_xml:parse_enumeration(ParseResult, ParserSpec);

parse_enumeration(Elem=#xmlElement{}, ParserSpec=#enum_parser_spec{}) ->
        ParseAcc = #parse_enum_acc{ spec = ParserSpec },

        case Elem#xmlElement.name of
          'EnumerationResults' ->
            Nodes = erlazure_xml:filter_elements(Elem#xmlElement.content),
            ParseAcc1 = ParseAcc#parse_enum_acc { misc = lists:foldl(fun parse_common_tokens/2, [], Nodes) },

            ParseAcc2 = lists:foldl(fun parse_list/2, ParseAcc1, Nodes),

            Items = lists:reverse(ParseAcc2#parse_enum_acc.items),
            Common = lists:reverse(ParseAcc2#parse_enum_acc.misc),
            Custom = lists:reverse(ParseAcc2#parse_enum_acc.custom),
            Misc = lists:append(Common, Custom),
            {ok, {Items, Misc}};

          _ -> {error, bad_response}
        end.

parse_list(Elem=#xmlElement{}, Acc=#parse_enum_acc{}) ->
        Root = Acc#parse_enum_acc.spec#enum_parser_spec.rootKey,
        Node = Acc#parse_enum_acc.spec#enum_parser_spec.elementKey,
        Parser = Acc#parse_enum_acc.spec#enum_parser_spec.elementParser,
        CustomParsers = Acc#parse_enum_acc.spec#enum_parser_spec.customParsers,
        case Elem#xmlElement.name of
          Root ->
            Nodes = erlazure_xml:filter_elements(Elem#xmlElement.content),
            lists:foldl(fun parse_list/2, Acc, Nodes);

          Node ->
            Items = Acc#parse_enum_acc.items,
            Acc#parse_enum_acc { items = [Parser(Elem) | Items] };

          Key ->
            case CustomParsers of
              [] ->
                Acc;

              CustomParsers ->
                case proplists:lookup(Key, CustomParsers) of
                  {_, CustomParser} ->
                    CustomItems = Acc#parse_enum_acc.custom,
                    Acc#parse_enum_acc { custom = [CustomParser(Elem) | CustomItems] };
                  _ ->
                    Acc
                end
            end
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
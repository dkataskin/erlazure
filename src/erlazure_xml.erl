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

-include_lib("xmerl/include/xmerl.hrl").

%% API
-export([get_element_text/2, parse_metadata/1, parse_list/2, filter_elements/1, get_text/1, parse_str_property/2,
         parse_int_property/2]).

get_element_text(ElementName, Elements) when is_list(ElementName), is_list(Elements) ->
            case lists:keyfind(ElementName, 1, Elements) of
              {ElementName, _, Value} -> lists:flatten(Value);
              false -> ""
            end.

parse_metadata([]) -> [];

parse_metadata(Elements) ->
            case lists:keyfind("Metadata", 1, Elements) of
              {"Metadata", _, MetadataElements} ->
                FoldFun = fun({Element, _, Value}, Acc) ->
                  [{Element, lists:flatten(Value)} | Acc]
                end,
                lists:foldl(FoldFun, [], MetadataElements);
              _ -> []
            end.

parse_list(ParseFun, List) ->
  FoldFun = fun(Element, Acc) ->
    [ParseFun(Element) | Acc]
  end,
  lists:reverse(lists:foldl(FoldFun, [], List)).

filter_elements(XmlNodes) ->
            lists:filter(fun(Elem) when is_record(Elem, xmlElement) -> true;
                            (_) -> false end, XmlNodes).

get_text(XmlElement) when is_record(XmlElement, xmlElement) ->
            [{xmlText, _, _, _, Text, _}] = XmlElement#xmlElement.content,
            Text.

parse_str_property(Property, XmlElement) when is_record(XmlElement, xmlElement) ->
            {Property, get_text(XmlElement)}.

parse_int_property(Property, XmlElement) when is_record(XmlElement, xmlElement) ->
            {Property, erlang:list_to_integer(get_text(XmlElement))}.
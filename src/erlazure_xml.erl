%%% Copyright (C) 2013 Dmitriy Kataskin
%%%
%%% This file is part of ErlAzure.
%%%
%%% ErlAzure is free software: you can redistribute it and/or modify
%%% it under the terms of the GNU Lesser General Public License as
%%% published by the Free Software Foundation, either version 3 of
%%% the License, or (at your option) any later version.
%%%
%%% ErlAzure is distributed in the hope that it will be useful,
%%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
%%% GNU Lesser General Public License for more details.
%%%
%%% You should have received a copy of the GNU Lesser General Public
%%% License along with ErlAzure.  If not, see
%%% <http://www.gnu.org/licenses/>.
%%%
%%% Author contact: dmitriy.kataskin@gmail.com
-module(erlazure_xml).
-author("dkataskin").

%% API
-export([get_element_text/2]).

get_element_text(ElementName, Elements) when is_list(ElementName), is_list(Elements) ->
            case lists:keyfind(ElementName, 1, Elements) of
              {ElementName, _, Value} -> lists:flatten(Value);
              false -> ""
            end.
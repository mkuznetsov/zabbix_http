%%%-------------------------------------------------------------------
%%% @author mike
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. Oct 2014 17:04
%%%-------------------------------------------------------------------
-module(zabbix_http).
-author("mike").

-behaviour(gen_fsm).
-include("zabbix_http.hrl").
%% API
-export([start_link/0, start_link/2,
         log/2,log/4, log/1, flush/0,
         set_host_port/2, set_interval/1, agregate/1, ts/0, hostname/0 ]).

%% gen_fsm callbacks
-export([init/1,
         idle/2,
         idle/3,
         collecting/2,
         collecting/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-define(SERVER, ?MODULE).
-define(HTTP_REQ_TIMEOUT,60000).
-define(COLLECTION_INTERVAL,30000).
-record(state, {cache, host,port, interval=?COLLECTION_INTERVAL, agregate=[]}).
%%--------------------------------------------------------------------
%% @doc
%% Set host and port to conect to Zabbix
%% @end
%%--------------------------------------------------------------------

set_host_port(Host,Port)->
  gen_fsm:sync_send_event(?SERVER,{set_host_port,Host,Port}).

%%--------------------------------------------------------------------
%% @doc
%% Set data buffer flush interval (seconds) for Zabbix metrics data. We buffer data, to reduce load on Zabbix and provide agregation for data.
%% Default interval is 30 sec.
%% @end
%%--------------------------------------------------------------------
set_interval(Interval) when is_number(Interval), Interval>0 ->
  gen_fsm:sync_send_event(?SERVER,{set_interval,Interval*1000}).

%%--------------------------------------------------------------------
%% @doc
%% Set list of metrics to predagregate for Zabbix. It is very usefull for counter related data (like queries per minute or money transactions). We can aggregate only numeric values. Agregation is a sum of listed metrics per node per interval.
%% By default data is not agregated.
%% To disable it call with [] as argument.
%% To enable it put list of metrics as argument, like this [<<"new_users">>, <<"cc_transactions_total">>]
%% @end
%%--------------------------------------------------------------------
agregate(MetricsList) when is_list(MetricsList) ->
  gen_fsm:sync_send_event(?SERVER,{agregate,MetricsList}).
%%--------------------------------------------------------------------
%% @doc
%% Force to flush inner buffer and send packet to zabbix
%% @end
%%--------------------------------------------------------------------
flush()->
  gen_fsm:sync_send_event(?SERVER,flush).

%%--------------------------------------------------------------------
%% @doc
%% Log Value by metric.
%% Source host by default current host name
%% @end
%%--------------------------------------------------------------------
log(Metric, Value)when is_binary(Metric), is_number(Metric)->
  log(Metric, Value, hostname(), ts()).

%%--------------------------------------------------------------------
%% @doc
%% Log Value by metric.
%% You can put custom Host and timestamp
%% @end
%%--------------------------------------------------------------------
log(Metric, Value, Hostname, Ts) when is_binary(Metric), is_number(Value), is_binary(Hostname), is_number(Ts)->
  log([{Metric, Value, Hostname, Ts}]) .

%%--------------------------------------------------------------------
%% @doc
%% Log list of events like log/2 and log/4.
%% Special hack - send only metrics name (hostname = current host, value=1,
%% @end
%%--------------------------------------------------------------------
log(Metric) when is_binary(Metric) ->
  log([{Metric,1}]);

log([{_K,_V}|_]=T) when is_binary(_K), is_number(_V) ->
  Ts=ts(),
  Host=hostname(),
  gen_fsm:send_event(?SERVER,[#event{metric = Metric, value = Value,
                                     host=Host, timestamp = Ts}||{Metric, Value} <-T]);

log([{_K,_V,_H,_Ts}|_]=T)->
  gen_fsm:send_event(?SERVER, [#event{metric = Metric, value = Value,
                                      host=Host, timestamp = Ts}||{Metric, Value, Host, Ts} <-T]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Start sender with Host and Port or without them
%% @end
%%--------------------------------------------------------------------
-spec(start_link() -> {ok, pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_fsm:start_link({local, ?SERVER}, ?MODULE, [], []).

start_link(Host,Port) ->
  gen_fsm:start_link({local, ?SERVER}, ?MODULE, [Host,Port], []).


stop() ->
  gen_fsm:sync_send_event(?SERVER, stop).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Init with Host and Port for Zabbix trapper.
%% If Host and Port not presented, you need to set it later with set_host_port
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
  {ok, StateName :: atom(), StateData :: #state{}} |
  {ok, StateName :: atom(), StateData :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([])->
  init([undefined,undefined]);

init([H,P]) ->
  {ok, idle, #state{cache = ets:new(zabbix_cache,[]), host=H,port=P}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Do nothing until get something. Than fire timer for flush and switch to collecting
%% @end
%%--------------------------------------------------------------------
-spec(idle(Event :: term(), State :: #state{}) ->
  {next_state, NextStateName :: atom(), NextState :: #state{}} |
  {next_state, NextStateName :: atom(), NextState :: #state{},
   timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
idle(Event, State) ->
  gen_fsm:send_event_after(State#state.interval, post_events),
  collecting(Event,State).
%%--------------------------------------------------------------------
%% @private
%% @doc
%% Collecting data and flush it after delay
%% @end
%%--------------------------------------------------------------------
-spec(collecting(Event :: term(), State :: #state{}) ->
  {next_state, NextStateName :: atom(), NextState :: #state{}} |
  {next_state, NextStateName :: atom(), NextState :: #state{},
   timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
collecting(#event{}=Event, State) ->
  collecting([Event], State);
collecting([#event{}|_]=Events, State=#state{agregate = []}) ->
  ets:insert(State#state.cache, [{make_ref(), Event}||Event<-Events]),
  {next_state, collecting, State};

collecting([#event{}|_]=Events, State=#state{agregate = MetricsList, cache = Tab}) ->
  [case lists:member(M, MetricsList) of
     true->
       ets:insert_new(Tab, {{M,H},Event, V}) orelse ets:update_counter(Tab,{M,H}, [{3, V}]);
     false->
       ets:insert(Tab, {make_ref(), Event})
   end||#event{metric=M,host=H, value=V}=Event<-Events],
  {next_state, collecting, State};
collecting(post_events, State=#state{cache = Tab, host=H,port = P}) ->
  Ts=ts(),
  EventList=[case E of
               {_,Event}->Event;
               {_,Event,V}->Event#event{value=V, timestamp=Ts}
             end||E<-ets:tab2list(Tab)],
  case send(H,P, to_json(EventList)) of
    ok->
      ets:delete_all_objects(Tab),
      {next_state, idle, State};
    {error, _Rsp }->
      % if we have an error or some other fail
      gen_fsm:send_event_after(State#state.interval, post_events),
      {next_state, collecting, State}
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sync events in idle state
%% Flush will not take any effect
%% @end
%%--------------------------------------------------------------------
-spec(idle(Event :: term(), From :: {pid(), term()},
           State :: #state{}) ->
            {next_state, NextStateName :: atom(), NextState :: #state{}} |
            {next_state, NextStateName :: atom(), NextState :: #state{},
             timeout() | hibernate} |
            {reply, Reply, NextStateName :: atom(), NextState :: #state{}} |
            {reply, Reply, NextStateName :: atom(), NextState :: #state{},
             timeout() | hibernate} |
            {stop, Reason :: normal | term(), NewState :: #state{}} |
            {stop, Reason :: normal | term(), Reply :: term(),
             NewState :: #state{}}).
idle({set_host_port,H,P}, _From, State) ->
  {_,Reply,_, NewState}=collecting({set_host_port,H,P}, _From, State),
  {reply, Reply, idle, NewState};

idle({set_interval, Interval}, _From, State)->
  {_,Reply,_, NewState}=collecting({set_interval, Interval}, _From, State),
  {reply, Reply, idle, NewState};

idle({agregate, MetricsList}, _From, State) ->
  {_,Reply,_, NewState}=collecting({agregate, MetricsList}, _From, State),
  {reply, Reply, idle, NewState};

idle(flush, _From, State) ->
  Reply = ok,
  {reply, Reply, idle, State};

idle(stop, _From, State) ->
  Reply = ok,
  {stop, normal, Reply, State}.
%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sync events in collecting state
%% @end
%%--------------------------------------------------------------------
-spec(collecting(Event :: term(), From :: {pid(), term()},
                 State :: #state{}) ->
                  {next_state, NextStateName :: atom(), NextState :: #state{}} |
                  {next_state, NextStateName :: atom(), NextState :: #state{},
                   timeout() | hibernate} |
                  {reply, Reply, NextStateName :: atom(), NextState :: #state{}} |
                  {reply, Reply, NextStateName :: atom(), NextState :: #state{},
                   timeout() | hibernate} |
                  {stop, Reason :: normal | term(), NewState :: #state{}} |
                  {stop, Reason :: normal | term(), Reply :: term(),
                   NewState :: #state{}}).
collecting({set_host_port, H,P}, _From, State) ->
  Reply = ok,
  {reply, Reply, collecting, State#state{host=H,port=P}};

collecting({set_interval, Interval}, _From, State) ->
  Reply = ok,
  {reply, Reply, collecting, State#state{interval = Interval}};

collecting({agregate, MetricsList}, _From, State) ->
  Reply = ok,
  {reply, Reply, collecting, State#state{agregate = MetricsList}};

collecting(flush, _From, State) ->
  Reply = ok,
  gen_fsm:send_event_after(10, post_events),
  {reply, Reply, collecting, State};

collecting(stop, _From, State) ->
Reply = ok,
{stop, normal, Reply, State}.

-spec(handle_event(Event :: term(), StateName :: atom(),
                   StateData :: #state{}) ->
                    {next_state, NextStateName :: atom(), NewStateData :: #state{}} |
                    {next_state, NextStateName :: atom(), NewStateData :: #state{},
                     timeout() | hibernate} |
                    {stop, Reason :: term(), NewStateData :: #state{}}).
handle_event(_Event, StateName, State) ->
  {next_state, StateName, State}.

-spec(handle_sync_event(Event :: term(), From :: {pid(), Tag :: term()},
                        StateName :: atom(), StateData :: term()) ->
                         {reply, Reply :: term(), NextStateName :: atom(), NewStateData :: term()} |
                         {reply, Reply :: term(), NextStateName :: atom(), NewStateData :: term(),
                          timeout() | hibernate} |
                         {next_state, NextStateName :: atom(), NewStateData :: term()} |
                         {next_state, NextStateName :: atom(), NewStateData :: term(),
                          timeout() | hibernate} |
                         {stop, Reason :: term(), Reply :: term(), NewStateData :: term()} |
                         {stop, Reason :: term(), NewStateData :: term()}).
handle_sync_event(_Event, _From, StateName, State) ->
  Reply = ok,
  {reply, Reply, StateName, State}.

-spec(handle_info(Info :: term(), StateName :: atom(),
                  StateData :: term()) ->
                   {next_state, NextStateName :: atom(), NewStateData :: term()} |
                   {next_state, NextStateName :: atom(), NewStateData :: term(),
                    timeout() | hibernate} |
                   {stop, Reason :: normal | term(), NewStateData :: term()}).

handle_info(_Info, StateName, State) ->
  {next_state, StateName, State}.

-spec(terminate(Reason :: normal | shutdown | {shutdown, term()}
| term(),       StateName :: atom(), StateData :: term()) -> term()).
terminate(_Reason, _StateName, _State) ->
  ok.

-spec(code_change(OldVsn :: term() | {down, term()}, StateName :: atom(),
                  StateData :: #state{}, Extra :: term()) ->
                   {ok, NextStateName :: atom(), NewStateData :: #state{}}).
code_change(_OldVsn, StateName, State, _Extra) ->
  {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
to_json(#event{metric = M,value = V, host = H, timestamp = T}) when is_binary(M), is_number(V), is_binary(H)->
  TsBin=integer_to_binary(T),
  VBin=integer_to_binary(V),
%%   <<"{\"host\":\"",H/binary,"\",\"key\":\"",M/binary,"\",\"value\":\"",VBin/binary,"\"}">>;
  <<"{\"host\":\"",H/binary,"\",\"key\":\"",M/binary,"\",\"value\":\"",VBin/binary,"\",\"clock\":",TsBin/binary,"}">>;


to_json([#event{}=Head|Rest])->
  JsonRest=[[<<",">>,to_json(Event)]||Event<-Rest],
  JsonEventList=list_to_binary([to_json(Head)|JsonRest]),
  Ts=integer_to_binary(ts()),
%%   <<"{\"request\":\"sender data\",\"data\":\[",JsonEventList/binary,"\]}">>.
  <<"{\"request\":\"sender data\",\"data\":\[",JsonEventList/binary,"\],\"clock\":",Ts/binary,"}">>.

ts()->
  {A,B,C}=erlang:now(),
  A*1000000000 + B*1000 + (C div 1000).
hostname()->
  {ok, Hostname} = inet:gethostname(),
  {ok,{hostent,FullHostname,[],inet,_,[_]}} = inet:gethostbyname(Hostname),
  list_to_binary(FullHostname).

send(undefined,undefined,_)->
  {error,incorrect_host_port_pair};

send(Host, Port, Data)->
  case gen_tcp:connect(Host, Port,
                       [{active,false},
                        {packet,0}]) of
    {ok, Socket} ->
      ok=gen_tcp:send(Socket, Data),
      ok=gen_tcp:close(Socket);
    Other ->
      {error, Other}
  end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
%% -export([test_workflow/0]).
workflow_test_() ->
  {foreach,
   fun() ->
     ok
   end,
   fun(_) ->
     ok
   end,
   [
     {<<"Typical flow">>,
      fun try_fsm_workflow/0
     }
     ,{<<"Inner flow">>,
      fun try_inner_workflow/0
     }
     ,{<<"Counter agregation">>,
       fun try_agragate/0
     }
   ]}.

try_inner_workflow()->
  Ts=ts(),
  H= <<"fmdev-e1.dev.fly.me">>,
  random:seed(now()),
  Val=random:uniform(20),
  Json= to_json([#event{metric = <<"3ds_authorize_declinded_by_antifrod-filter">>,value = Val, host = H, timestamp = Ts}]),
  ?debugFmt("Json to send ~s",[Json]),
  Status=send("zabbix.dev.fly.me",10051, Json),
  ?debugFmt("Status ~p",[Status]),
  ?assertEqual(ok,Status),
  ok.

try_fsm_workflow()->
  H= <<"fmdev-e1.dev.fly.me">>,
  random:seed(now()),
  Val=random:uniform(20),

  start_link(),
  set_host_port("zabbix.dev.fly.me",10051),
  Status = log([{<<"3ds_authorize_declinded_by_antifrod-filter">>,Val,H, ts()}]),
  flush(),
  timer:sleep(200),
  ?debugFmt("Status val=~p status=~p",[Val,Status]),
  ?assertEqual(ok,Status),
  stop(),
  ok.

try_agragate()->
  H= <<"fmdev-e1.dev.fly.me">>,
  random:seed(now()),
  Val1=random:uniform(20),
  Val2=random:uniform(20),
  Val3=random:uniform(20),

  start_link(),
  set_host_port("zabbix.dev.fly.me",10051),
  agregate([<<"3ds_authorize_declinded_by_antifrod-filter">>]),
  Status = [log([{<<"3ds_authorize_declinded_by_antifrod-filter">>,Val1,H, ts()}]),
            log([{<<"3ds_authorize_declinded_by_antifrod-filter">>,Val2,H, ts()}]),
            log([{<<"3ds_authorize_declinded_by_antifrod-filter">>,Val3,H, ts()}])],
  flush(),
  timer:sleep(200),
  ?debugFmt("Status val=~p status=~p",[Val1+Val2+Val3,Status]),
  ?assertEqual([ok,ok,ok],Status),
  stop(),
  ok.
-endif.
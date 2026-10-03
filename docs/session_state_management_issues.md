# Session state managment issues

The current babelfix session layer provides apps with a series of events that
include both things they must react to and updates on the the persistent session
state.

But this is not currently robust.

In order for a FIX application to be robust it must satisfy the following
recovery properties:

- A trigger event (e.g. order entry, amened, cancel) must only ever be
  procseed once, but the resulting message must be recoverable at least for the
  lifetime of its FIX session. Only the application can determine what this
  means (e.g. store the messages in a database and replay them as-is, or
  re-generated them from the stored trigger events). Only the app can decide if
  a re-requested message should be gap-filled.
- Recovery/replay requires knowledge of which previous sequence numbers where
  gap-fillable administrative messages.
- A received message must only be marked as "processed" (i.e. the "next inbound
  sequence number" used on recovery advanced past it) once the application has
  persisted sufficient state related to it that it should never be re-requested.
  Only the application can determine what this means.

The design of babelfix attempts to satisfy these properties, or rather allow
applications to satisfy them, by:

- Providing each wire message (rx or tx) to the app as an event, which the app
  can serialise
- asking the app to satisfy replay requests with an api like
  session.resend(message)/session.resend_complete() and auotmatically gap-fill
  any skipped seqno
- periodically providing the next in and next out sequence numbers to the app
  before every in-order processed message.

But there are problems with this:

- The app gets SessionState _before_ MessageReceived. Meaning if it persists
  this state then crashes, it will have claimed to have processed a message that
  it never actually received as MessageReceived.
- RawMessageSent is the only way to discover the sequence number and sending
  time (i.e. the wire message) for a given trigger event (sent message), but
  this happens _after_ the message has been sent. This means that if the app
  sends a message then crashes, the counterparty might receive it but the app
  has no memory of having send it. Then the next connection attempt will be at a
  _lower sequence number_ than the counterparty expects - a fatal recovery error
  in FIX and a strict violation. Worse, the app might re-send the trigger event
  causing a duplicate order or trade.

Further, a lot of this design assumes that application's have persistence and
that persistence is somehow _synchronous_. But in reality robust systems
persistence requires a replication to a standby or some other form of
asynchronous operation, which can be both costly and 'async' in the state model
sense.

In previous FIX engines i've desgigned and worked with, they either fully work
on "a message is just a message" or full on "an event stream defines the sequence, and
sequence numbers are implicit in the order of events".

In the former case, a trigger event (application event) is processed, a FIX
message is created, and the sequence number is stamped on the
message, the message is assigned to a FIX sesssion (a series of messages starting
from logon seqno 1), and _persisted_ by the application _before_ sending the
message to the counterparty. Recovery then works entirely on the persisted
messages only, without any application semantic state.

The latter design integrates more closely with how the application itself
produces messages, and works excellently for event driven systems where "some
event" is created which triggers the construction of the FIX message. For
example an order entry system writes an "order entry requested" event and the
FIX engine is responsible for turning that into a FIX message and sending it
to the counterparty. The advanage is that the FIX engine can be "stateless" in
the sense that it needs not create another pesisted transaction in order to
dispatch the message (the act of persisting the 'order entry requested' event is
sufficient to implicitly assign a sequence number to the _event_ and an _event_
may produce exactly 1 FIX message).

The typical tension with this latter design in FIX engines is "who assigns the
sequence number" and "who owns the 'next in/next out' state". It's tempting for
the FIX session layer to do that, and even babelfix is guilty of this to some
extent, but one option for robustness is for the application itself to be solely
responsible for the sequence number of a given outbound message and for the
persisted session state.

This has a couple of consequences:

- The session layer, when it wants to Send an administrative message, must ask
  the application to do so. This is a sort of loopback, where the app is told "i
  need to send $message now, please do whatever you need to do then call
  session.Send($message) with the apporpirately set sequence number.
- An application trigger must be associated with a specific FIX sesssion (by
  which we mean a specific series of messages starting from seqno 1, i.e. after
  the bilaterally agreed sequence number reset time aka "session reset time"
  etc.), and each trigger, including admin message sent by the session layer,
  must be ordered within that session (the sequence number then being N where N
  is the ordinal position of the trigger in the list of events for that
  session).

For the receive path, the 2 models (message-centric vs event-centric) have
similar trade-offs. 

Message-centric means storing/persisting the received message and declaring
that "processed" at the FIX engine layer, then having a protocol for the
application to request re-processing of a message were it to crash before it had
persisted sufficient state to declare it "processed" in its own right. Other
methods can be imagined here, but typical "message centric" FIX engines do this
sort of store-and-forward because they think in messages and sequences, but not
applciation events.

The Event-centric design requires that the application use the seqeuence number
(and session identifier/start point) as a sort of "transaction id" for the event
that produced the resulting event (e.g. order execttion, ack, etc.), and the
"next inbound sequence number" expected is defined as the next one after the
last event that was successfully processed by the application's business event
handler (and persistence layer).

it should be obvious that the event-centric design has a huge advantage over the
message-centric design which is that it can avoid entirely a requirement for
'synchronous' persistence at the "FIX layer". If the sequence number of a given
Event (which is already persisted) is implicit in that event's position in the
stream, then there is no requirement to persist anything - just send the FIX
message generated from the event. Indeed, in this model you don't even need to
persist the 'last sent sequence number' anywhere, because the last-sent seqence
number need not actually be the last one sent, but the latest _event_ produced.
A counterparty will re-request any messages it has not received, including ones
you might have never sent; it's perfectly legitimate to send a logon (seqno =
100) after previously logout(seqno = 90) and the counterparty will req-request
91-100, allowing the application to decide if those events should be replayed or
gap filled. The events happened, but were not assigned to a FiX session until
the application sent them. The only thing the app needs to persist is:

- the transaction IDs (session id + sequence number) of reeceived and procssed events
- the event stream ID of the first event in the current FIX session (the one with
  sequence number 1); this is used to implicitly determine the sequence numbers
  of subsequent events in the stream.

administrative messages appear as entries in the outbound event stream, and can
usually be simply ignored by the application on the inbound stream, because
re-requesting their sequcne numbers is usually not a problem (they are
gap-fillable).

The obvious downside of this event-centric design is that it requires often a
deeeper integration with application design, persistence and discipline than the
message-centric one. It requires that the session state management and seqno
discipline are designed strictly into the app and the FIX engine can provide
relatively little assistance in this. 

So I think I would like babelfix to support both paradigms. The way to do that
is to by default, support the event-centric "app owns sequence nunbers and
recovery state" model, but provide the apprporiate hooks for applications to
actually work in a message-centric way. It mostly comes down to more state model
events. And note that in the case of 'event-centric', the persistence of raw
messages (rx and tx) in context is still essential : applications will need to
be able to associate the exact FIX message they craeted with the event that
created it for the purposes of user display, auditing, debugging and compliance.

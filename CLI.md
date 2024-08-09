<table>
  <tr>
    <th><b>Command</b></th>
    <th><b>Description</b></th>
    <th><b>Output</b></th>
  </tr>
  <tr>
    <td><code>create topic &lt;id&gt; &lt;type&gt;</code></td>
    <td>
      <ul>
        <li>Creates a new topic in the tributary.</li>
        <li><code>id</code> is the topic’s identifier.</li>
        <li>
          <code>type</code> is the type of event that goes through the topic.
          While this can be any type in Java, for the purposes of the CLI it can
          either be <code>Integer</code> or <code>String</code>.
        </li>
      </ul>
    </td>
    <td>
      A message showing the id, type and other relevant information about the
      topic confirming its creation.
    </td>
  </tr>
  <tr>
    <td><code>create partition &lt;topic&gt; &lt;id&gt;</code></td>
    <td>
      <ul>
        <li>
          Creates a new partition in the topic with id <code>topic</code>.
        </li>
        <li><code>id</code> is the partition’s identifier.</li>
      </ul>
    </td>
    <td>A message confirming the partition’s creation.</td>
  </tr>
  <tr>
    <td>
      <code>create consumer group &lt;id&gt; &lt;topic&gt; &lt;rebalancing&gt;</code>
    </td>
    <td>
      <ul>
        <li>Creates a new consumer group with the given identifier.</li>
        <li>
          <code>topic</code> is the topic the consumer group is subscribed to.
        </li>
        <li>
          <code>rebalancing</code> is the consumer group’s initial rebalancing
          method, one of <code>Range</code> or <code>RoundRobin</code>.
        </li>
      </ul>
    </td>
    <td>A message confirming the consumer group’s creation.</td>
  </tr>
  <tr>
    <td><code>create consumer &lt;group&gt; &lt;id&gt;</code></td>
    <td>
      <ul>
        <li>Creates a new consumer within a consumer group.</li>
      </ul>
    </td>
    <td>A message confirming the consumer’s creation.</td>
  </tr>
  <tr>
    <td><code>delete consumer &lt;consumer&gt;</code></td>
    <td>
      <ul>
        <li>Deletes the consumer with the given id.</li>
      </ul>
    </td>
    <td>
      A message confirming the consumer’s deletion, and an output of the
      rebalanced consumer group that the consumer was previously in.
    </td>
  </tr>
  <tr>
    <td>
      <code>create producer &lt;id&gt; &lt;type&gt; &lt;allocation&gt;</code>
    </td>
    <td>
      <ul>
        <li>Creates a new producer which produces events of the given type.</li>
        <li>
          <code>allocation</code> is either <code>Random</code> or
          <code>Manual</code>, determining which method of partition selection
          is used for publishing events.
        </li>
      </ul>
    </td>
    <td>A message confirming the producer’s creation.</td>
  </tr>
  <tr>
    <td>
      <code>produce event &lt;producer&gt; &lt;topic&gt; &lt;event&gt; &lt;partition&gt;</code>
    </td>
    <td>
      <ul>
        <li>
          Produces a new event from the given producer to the given topic.
        </li>
        <li>
          How you represent the event is up to you. We recommend using a JSON
          structure to represent the different parts of an event and the
          <code>event</code> parameter to this command is a filename to a JSON
          file with the event content inside.
        </li>
        <li>
          <code>partition</code> is an optional parameter used only if the
          producer publishes events to a manually specified partition which
          isn't stored inside the message structure
        </li>
      </ul>
    </td>
    <td>The event id, the id of the partition it is currently in.</td>
  </tr>
  <tr>
    <td><code>consume event &lt;consumer&gt; &lt;partition&gt;</code></td>
    <td>
      <ul>
        <li>
          The given consumer consumes an event from the given partition.
          Precondition: The consumer is already allocated to the given
          partition.
        </li>
      </ul>
    </td>
    <td>
      The id and contents of the event, showing that the consumer has received
      the event.
    </td>
  </tr>
  <tr>
    <td>
      <code>consume events &lt;consumer&gt; &lt;partition&gt; &lt;number of events&gt;</code>
    </td>
    <td>
      <ul>
        <li>Consumes multiple events from the given partition.</li>
      </ul>
    </td>
    <td>The id and contents of each event received in order.</td>
  </tr>
  <tr>
    <td><code>show topic &lt;topic&gt;</code></td>
    <td></td>
    <td>
      Prints a visual display of the given topic, including all partitions and
      all of the events currently in each partition.
    </td>
  </tr>
  <tr>
    <td><code>show consumer_group &lt;group&gt;</code></td>
    <td></td>
    <td>
      Shows all consumers in the consumer group, and which partitions each
      consumer is receiving events from.
    </td>
  </tr>
  <tr>
    <td>
      <code>parallel produce &lt;producer1&gt;,&lt;topic1&gt;,&lt;event1&gt;|&lt;producer2&gt;,&lt;topic2&gt;,&lt;event2&gt;... </code>
    </td>
    <td>
      <ul>
        <li>
          Produces a series of events in parallel. This is purely for
          demonstrating that your tributary can cope with multiple producers
          publishing events simultaneously.
        </li>
      </ul>
    </td>
    <td>For each event, the id of the partition it is currently in.</td>
  </tr>
  <tr>
    <td>
      <code> parallel consume &lt;consumer1&gt;,&lt;partition1&gt;|&lt;consumer2&gt;,&lt;partition2&gt; </code>
    </td>
    <td>
      <ul>
        <li>
          Consumes a series of events in parallel. This is purely for
          demonstrating that your tributary can cope with multiple consumers
          receiving events simultaneously.
        </li>
      </ul>
    </td>
    <td>For each event consumed, the contents of the event and its id.</td>
  </tr>
  <tr>
    <td>
      <code>set consumer_group_rebalancing &lt;group&gt; &lt;rebalancing&gt;</code>
    </td>
    <td>
      <ul>
        <li>
          Updates the rebalancing method of consumer group <code>group</code> to
          be one of <code>Range</code> or <code>RoundRobin</code>.
        </li>
      </ul>
    </td>
    <td>A message confirming the new rebalancing method.</td>
  </tr>
  <tr>
    <td>
      <code>playback &lt;consumer&gt; &lt;partition&gt; &lt;offset&gt;</code>
    </td>
    <td>
      <ul>
        <li>Plays back events for a given consumer from the offset.</li>
      </ul>
    </td>
    <td>The id and contents of each event received in order.</td>
  </tr>
</table>

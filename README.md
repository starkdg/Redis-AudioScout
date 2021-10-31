# Redis-AudioScout

Redis module for audio track recognition.  This module provides command
extensions for the fast indexing and matching of audio fingerprints.
The recognition is robust to distortion.

## How It Works

To read more about how it works, read the following blog post
[here](htts://starkdg.github.io/posts/audioscout) which discusses the
[Java implementation](https://github.com/starkdg/JAudioScout).
The Redis implementation works the same way but takes advantage of
the fast efficient Redis core, and should be much simpler to deploy.

## Commands

The module provides a very simple interface that enables the adding,
deleting and identification of audio tracks.  The commands are
largely self explanatory.

```
auscout.add key <hasharray>
auscout.addtrack key <hasharray> <descr>
```

Add an audio fingerprint to the index. Returns integer ID assigned
to the new entry.  The hasharray is the audio fingerprint of the
signal, an array of 32-bit integers.  The module expects these
integers to be in network-byte-order.  `<descr>` is a string to
annotate the entry and is returned for matched query results.
The operation is O(N) where N is the length of the hash array.

```
auscout.del key <idvalue>
```

Delete the entry.  Returns the number of frames deleted from the index.
This is also O(N), where N is the number of frames of the indexed track.  

```
auscout.lookup key <hasharray> <togglearray> [threshold]
```

Query command to find the matching result for a given fingerprint.
The hasharray is the audio fingerprint, an array of 32-bit integers.
The toggle array is also an array of 32-bit integers that are bitmaps
denoting the  positions most likely to flip in distortion. Each toggle
value has equal number of set bits. Both these arrays are obtained from
the audiohash function in libpHashAudo.  The module expects both the
hasharray and togglesarray values to be in network-byte-order.

The command returns an array of arrays with each inner array consisting of
[descr, id, position, score].  The descr element is the annotated string.
The id is its assigned integer id.  The position is an integer offset denoting
the starting position in the matched fingerprint.  There is a function in the
audiohash library to convert the position into number of seconds.

Complexity is O(N*2^P), where N is the length of the hasharray, and P is
the number of toggles, or set bit positions in the toggle array.  Each toggle
array element will have the same number of set bit positions.

```
auscout.count key
auscout.size key
```

`count` returns the number of entries in the index. `size` returns the total
number of frames indexed.  Complexity is O(1).

```
auscout.delkey key
```

Deletes the key.  Use is encouraged in place of `del` command, since it deletes all
id hash descr fields in addition to the index itself.  On the other hand, if you want
to keep the id hash fields while deleting the index, use `del`.  Returns a string
acknowledgement.  Complexity is O(N*M), where N is the number of indexed tracks,
and M is the average number of indexed frames per track. 


```
auscout.index key
auscout.list key

```

Some debugging commands.  `auscout.index` will log the whole index to the log file,
and `auscout.list` will log the list of submitted fingerprints. Only use with small
indices in order to debug problems.


# AuscoutClient Program

Use the `auscoutclient` utility to interact with the module. It
operates in three commands: add, del or lookup.  A directory containing the audio
files can be specified with the -d or --dir option. 


To index a directory of audio files:

```
./auscoutclient add -d /path/to/audio/files -k mykey  -s localhost -p 6379
```

To delete a track from the index:

```
./auscoutclient del -k mykey --id <idvalue> 
```

To lookup audio clips in a directory:

```
./auscoutclient lookup -k mykey --dir /path/to/audio/clips --threshold 0.10 -t 4
```

Use `./auscoutclient -h` to get a complete list of the options available.


## Dependencies

The module has no dependencies other than redis itself.  However, the client
program needs the following:

[libAudioData](https://github.com/starkdg/libAudioData)

[libpHashAudio](https://github.com/starkdg/libpHashAudio)

[hiredis](https://github.com/redis/hiredis)

[Boost 1.67](https://www.boost.org/) the filesystem, program_options, system components

libAudioData reads the source audio signal from files and is a wrapper around libmp123,
libsndfile and libsamplerate libraries.  Local installation of those libraries
are required as well.  

libpHashAudio provides the functions to calculate the audio fingerprints.

## Installation Instructions

Build and run the server from Docker:

```
docker build --tag auscout:0.1 .
docker run --detach --publish 6379:6379 --mount src=auscoutdata,dst=/data --name auscout auscout:0.1
```

Compile from source:


```
make
make all
make install
```

This compiles the client demo program.

Installs to local /usr/local/lib directory

Load the module:

```
module load /var/local/lib/auscout.so
```

Or put this in your redis.conf configuration file:

```
loadmodule /var/local/lib/auscout.so
```

Run `testclient` with a local running redis-server to run basic tests.



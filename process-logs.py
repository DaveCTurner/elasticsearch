import json
import argparse
import re

parser = argparse.ArgumentParser(description='Process SnapshotStressTestsIT failure logs')
parser.add_argument(
  '--file',
  dest='filename',
  help='file to parse',
  required=True
  )
parser.add_argument(
  '--index',
  dest='index',
  help='problematic index name',
  required=True
  )
parser.add_argument(
  '--shard',
  dest='shard',
  help='problematic shard id',
  type=int,
  required=True
  )
args = parser.parse_args()

clusterStateVersionRegex = re.compile('.*Publishing cluster state version \[([0-9]+)\].*')
repositoryDataRegex = re.compile('.*\[(repo-[0-9]+)\] writing new RepositoryData \[index-([0-9]+)\]: (.*)')

snapshotOrder = {}
snapshotSeq = []
clusterStateVersion = 0

with open(args.filename) as f:
    for line in f:
        line = line.replace('  1> ', '').strip()
        if line.startswith('SnapshotsInProgress: '):
            snapsInProgress = json.loads(line[len('SnapshotsInProgress: '):])
            snapshotsByKey = {}
            newSnapshots = False
            for repoName, repoEntries in snapsInProgress.items():
                for repoEntry in repoEntries:
                    snapshot = repoEntry['entry']
                    snapKey = snapshot['repository'][-2:] + "/" + snapshot['snapshot'][-6:]
                    if snapKey not in snapshotOrder:
                        snapshotOrder[snapKey] = len(snapshotOrder)
                        snapshotSeq.append(snapKey)
                        newSnapshots = True
                    snapshotsByKey[snapKey] = snapshot

            if newSnapshots:
                print(str(clusterStateVersion) + ' . . ', end='')
                for snapshotKey in snapshotSeq:
                    if snapshotKey in snapshotsByKey:
                        print('---> ' + snapshotKey + ' ', end='')
                    else:
                        print('. . ', end='')
                print()

            print(str(clusterStateVersion) + ' . . ', end='')
            for snapshotKey in snapshotSeq:
                snapshot = snapshotsByKey.get(snapshotKey)
                if snapshot is None:
                    print('- - ', end='')
                else:
                    print(snapshot['state'][:4], ' ', end='')
                    foundShard = False
                    if not foundShard:
                        for shard in snapshot['shards']:
                            if shard['index']['index_name'] == args.index and shard['shard'] == args.shard:
                                foundShard = True
                                shardState = str(shard.get('state', 'OMIT'))
                                shardGen   = str(shard.get('generation', 'OMIT'))
                                print(shardState[:4] + '/' + shardGen[:4] + ' ', end='')
                                break
                    if not foundShard:
                        for shard in snapshot.get('clones', []):
                            if shard['index']['name'] == args.index and shard['shard'] == args.shard:
                                foundShard = True
                                shardState = str(shard.get('state', 'OMIT'))
                                shardGen   = str(shard.get('generation', 'OMIT'))
                                print(shardState[:4] + '/' + shardGen[:4] + ' ', end='')
                                break
                    if not foundShard:
                        print('- ', end='')
            print()
            next

        clusterStateVersionMatch = re.fullmatch(clusterStateVersionRegex, line)
        if clusterStateVersionMatch is not None:
            clusterStateVersion = int(clusterStateVersionMatch.group(1))
            next

        repositoryDataMatch = re.fullmatch(repositoryDataRegex, line)
        if repositoryDataMatch is not None:
            repository = repositoryDataMatch.group(1)
            gen = int(repositoryDataMatch.group(2))
            repositoryData = json.loads(repositoryDataMatch.group(3))
            shardGens = repositoryData.get('indices', {}).get(args.index, {}).get('shard_generations', [])
            shardGen = shardGens[args.shard] if args.shard < len(shardGens) else None
            shardGen = '-' if shardGen is None else shardGen[:4]
            print(clusterStateVersion, repository, gen, shardGen)
            next



#!/usr/bin/python
"""Usage: splitfasta.py <fasta_file> [--batch-size=1000] [--debug]

Options:
  --batch-size=<N>  Split into batches of N records [default: 1000].
"""
from docopt import docopt
from Bio import SeqIO


def split_fasta(fasta_file, batch_size):
    record_iter = SeqIO.parse(open(fasta_file), "fasta")
    for i, batch in enumerate(batch_iterator(record_iter, batch_size)):
        filename = f"group_{i+1}.fasta"
        with open(filename, "w") as handle:
            SeqIO.write(batch, handle, "fasta")


def batch_iterator(iterator, batch_size):
    """Returns lists of length batch_size.

    This can be used on any iterator, for example to batch up
    SeqRecord objects from Bio.SeqIO.parse(...), or to batch
    Alignment objects from Bio.AlignIO.parse(...), or simply
    lines from a file handle.

    This is a generator function, and it returns lists of the
    entries from the supplied iterator.  Each list will have
    batch_size entries, although the final list may be shorter.

    https://biopython.org/wiki/Split_large_file
    """
    entry = True  # Make sure we loop once
    while entry:
        batch = []
        while len(batch) < batch_size:
            try:
                entry = iterator.__next__()
            except StopIteration:
                entry = None
            if entry is None:
                # End of file
                break
            batch.append(entry)
        if batch:
            yield batch


if __name__ == "__main__":
    args = docopt(__doc__)
    file = args.get('<fasta_file>')
    size = int(args.get('--batch_size', 1000))
    print(f"Splitting {file} into files containing {size} sequences each.")
    split_fasta(file, size)


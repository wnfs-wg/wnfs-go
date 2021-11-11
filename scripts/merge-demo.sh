rm -R ./alice ./bob | true

echo "\n1. create Alice's filesystem"
echo "mkdir alice && cd alice\n"
mkdir alice && cd alice

echo "\n2. add a file to private & public directories:"
echo "this is alice's file" > alice.txt
echo "wnfs cp /private/alice.txt alice.txt\n"
wnfs cp /private/alice.txt alice.txt
echo "wnfs cp /public/alice.txt alice.txt"
wnfs cp /public/alice.txt alice.txt

echo "\n3. copy Alice's repo, renaming to bob"
# _README file has read-only permissions, needs to be removed to avoid copying
# error
rm -rf .wnfs/ipfs/blocks/_README
cp -R ../alice ../bob

echo "\n4. modify Alice's filesystem by adding a new file:"
echo "wnfs cp /private/hello.txt hello.txt"
echo "hello!" > hello.txt
wnfs cp /private/hello.txt hello.txt

echo "Alice's file tree:\n"
wnfs tree

echo "\n5. switch to bob's filesystem, add files:"
echo "cd ../bob"
cd ../bob
echo "wnfs cp /private/bob.txt bob.txt"
echo "bob file" > bob.txt
wnfs cp /private/bob.txt bob.txt
# echo "wnfs cp /public/bob.txt bob.txt"
# wnfs cp /public/bob.txt bob.txt

echo "Bob's file tree:\n"
wnfs tree

echo "\n6. switch back to Alice & merge in Bob's changes"
echo "cd ../alice\n"
cd ../alice
echo "wnfs merge ../bob/\n"
wnfs merge ../bob/

echo "Alice's merged file tree:\n"
wnfs tree
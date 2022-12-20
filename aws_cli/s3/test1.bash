s3list=`aws s3 ls | awk  '{print $3}'`
for s3dir in $s3list
do
    echo $s3dir
    aws s3 ls "s3://$s3dir"  --recursive --human-readable --summarize | grep "Total Size"
    aws s3 ls "s3://$s3dir"  --recursive --human-readable --summarize | grep "Total Object"
done



aws s3api list-objects --bucket YOUR_BUCKET --output text --query "Contents[].{Key: Key}"

aws s3 ls "s3://sydev-lake"  --recursive --human-readable --summarize


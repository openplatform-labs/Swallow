/** Verify cursor ordering before exposing a channel page. */
export function validChannelPage(rows: {jobId:string}[], hasMore: boolean, next: unknown, after: string|null): boolean {
  const ids=rows.map(row=>row.jobId.toLowerCase());
  if (ids.some((id,index)=>(index>0 && ids[index-1]>=id) || (after!==null && id<=after.toLowerCase()))) return false;
  return hasMore ? ids.length===20 && typeof next==='string' && next.toLowerCase()===ids[ids.length-1] : next===null;
}

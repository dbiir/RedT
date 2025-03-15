1) In \texttt{redtatom.tla}, we simulate the simultaneous failure of the coordinator and fewer than $\frac{1}{4}$ of the replicas.
Subsequently, following the proof in Section \ref{sec:co.ad}, we verify the \textit{atomicity} invariant, which requires that after the transaction finishes, more than $\frac{3}{4}$ of the replicas must reach a consistent state with the coordinator, either committed or aborted.
Then, we verify the \textit{durability} invariant, which mandates that throughout system execution, more than $\frac{3}{4}$ of the replicas for each data item must maintain consistent data.
2) In \texttt{RedTSI.tla}, we primarily examine two criteria to check whether transaction executions satisfy the requirements of SI:
\textit{snapshot read} criteria validates that after a read transaction accesses a data item, no version written with a timestamp preceding the read transaction's snapshot time exists.
Fulfillment of this condition, i.e., the absence of versions written with earlier timestamps, indicates that the read transaction satisfies the \textbf{SI1} condition as defined in Section \ref{sec:si-co}.
\textit{snapshot isolation write} criteria verifies the absence of overlapping execution intervals between any two write transactions accessing the same data item.
The absence of such overlaps indicates that these write transactions satisfy the \textbf{SI2} condition as defined in Section \ref{sec:si-co}.
3) In \texttt{RedTSER.tla}, we examine the serializability criteria, which verifies whether cycles exist in the dependency graph constructed from concurrently running transactions.

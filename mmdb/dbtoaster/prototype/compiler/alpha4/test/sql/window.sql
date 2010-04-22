CREATE TABLE R(t int, x float);
CREATE TABLE W(w int);

SELECT   sum(R.x)
FROM     R,W 
WHERE    R.t <= W.w AND R.t > W.w + (-30)
GROUP BY W.w

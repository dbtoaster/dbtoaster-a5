set term pdf enhanced
set output 'broken_pdf.pdf'
set ytics
set xtics (-2,-1,1,2)
unset key
set zeroaxis
set xrange [-3:3]
set yrange [-.01:.17]
set samples 1000
plot (abs(x) < 2 && abs(x) > 1) ? (1/(2*pi)*exp(-x*x / 2)) : 0 with filledcurve, (abs(x) < 2 && abs(x) > 1) ? (1/(2*pi)*exp(-x*x / 2)) : 0 with line lt 0, (1/(2*pi)*exp(-x*x / 2)) lt 0 lw 3
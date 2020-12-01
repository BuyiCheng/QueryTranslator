$(function(){
    $(".result").hide()
})

$(".btn-translate").on('click',function(){
    translation_type = $("select.languages option:selected").text()
    sql = $("#SQL").val()
    // alert(sql)
    // alert(translation_type)
    $.ajax({
        async: true,
        type: 'POST',
        url: '/translate',
        dataType:"json",
        data: {'type':translation_type, 'sql': sql},
        success:function(result){
            $(".result").show()
            $("#tl-result").empty()
            $("#result-title").text(translation_type + ' Result')
            if (translation_type == 'MongoDB'){
                $("#tl-result").text(result.tl)
            } else if(translation_type == 'Pandas Dataframe'){
                s_list = result.tl
                for( i in s_list) {
                    $("#tl-result").append("<pre>"+s_list[i]+"</pre>")
                }
            }

            
            console.log(result.tl)
        },
        error: function(error){
            alert('error:'+error)
        }
    })
});

$(".close").on('click', function(){
    $(".result").hide()
})